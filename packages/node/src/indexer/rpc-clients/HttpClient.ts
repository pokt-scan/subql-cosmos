// // Copyright 2020-2025 SubQuery Pte Ltd authors & contributors
// SPDX-License-Identifier: GPL-3.0

import http from 'http';
import https from 'https';
import {
  isJsonRpcErrorResponse,
  JsonRpcRequest,
  JsonRpcSuccessResponse,
  parseJsonRpcResponse,
} from '@cosmjs/json-rpc';
import { HttpEndpoint } from '@cosmjs/tendermint-rpc';
import axios, { AxiosInstance } from 'axios';

import { RpcClient } from './RpcClient';

// eslint-disable-next-line @typescript-eslint/no-var-requires
const simdjson = require('simdjson');

// Read HTTP timeout from environment variable, undefined if not set
const httpTimeout = process.env.HTTP_CLIENT_TIMEOUT
  ? parseInt(process.env.HTTP_CLIENT_TIMEOUT, 10)
  : undefined;

export function hasProtocol(url: string): boolean {
  return url.search('://') !== -1;
}

// Maximum chunk size for JSON.parse (must stay under V8's 512MB string limit)
const MAX_CHUNK_BYTES = 400 * 1024 * 1024;

// BlockResultsResponse fields that can be large arrays
const ARRAY_FIELDS = [
  'txs_results',
  'begin_block_events',
  'end_block_events',
  'finalize_block_events',
];

// BlockResultsResponse fields that are scalars or small objects
const SCALAR_FIELDS = [
  'height',
  'validator_updates',
  'consensus_param_updates',
  'app_hash',
];

const ALL_RESULT_FIELDS = [...new Set([...ARRAY_FIELDS, ...SCALAR_FIELDS])];

/**
 * Extract a scalar/object value from a JSON buffer by searching for its key.
 * Handles strings, objects, arrays, null, true, false, and numbers.
 * Returns undefined if the key is not found.
 */
function extractScalarFromBuffer(
  buf: Buffer,
  field: string,
  searchFrom: number,
): any {
  const keyBuf = Buffer.from(`"${field}":`);
  const keyPos = buf.indexOf(keyBuf, searchFrom);
  if (keyPos === -1) return undefined;

  const valStart = keyPos + keyBuf.length;
  const firstByte = buf[valStart];
  let valEnd: number;

  if (firstByte === 0x22) {
    // String: find closing unescaped quote
    let pos = valStart + 1;
    while (pos < buf.length) {
      if (buf[pos] === 0x5c) {
        pos += 2;
        continue;
      }
      if (buf[pos] === 0x22) {
        pos++;
        break;
      }
      pos++;
    }
    valEnd = pos;
  } else if (firstByte === 0x7b || firstByte === 0x5b) {
    // Object or Array: track depth
    const close = firstByte === 0x7b ? 0x7d : 0x5d;
    let depth = 1,
      pos = valStart + 1,
      inStr = false;
    while (pos < buf.length && depth > 0) {
      if (inStr) {
        if (buf[pos] === 0x5c) pos++;
        else if (buf[pos] === 0x22) inStr = false;
      } else {
        if (buf[pos] === 0x22) inStr = true;
        else if (buf[pos] === firstByte) depth++;
        else if (buf[pos] === close) depth--;
      }
      pos++;
    }
    valEnd = pos;
  } else if (firstByte === 0x6e) {
    valEnd = valStart + 4; // null
  } else if (firstByte === 0x74) {
    valEnd = valStart + 4; // true
  } else if (firstByte === 0x66) {
    valEnd = valStart + 5; // false
  } else {
    // number
    let pos = valStart;
    while (
      pos < buf.length &&
      buf[pos] !== 0x2c &&
      buf[pos] !== 0x7d &&
      buf[pos] !== 0x5d
    )
      pos++;
    valEnd = pos;
  }

  return JSON.parse(buf.toString('utf8', valStart, valEnd));
}

/**
 * Parses a JSON-RPC block_results response Buffer that exceeds V8's 512MB string limit.
 *
 * Uses simdjson's findChunkBoundaries (C++ state machine) to locate array element
 * boundaries, then feeds <400MB chunks to V8's native JSON.parse.
 * Scalar fields are extracted directly from the buffer via byte search.
 *
 * No lazyParse/valueForKeyPath — those cause segfaults or 200s+ delays on Node 18.
 */
function parseLargeJsonBuffer(buf: Buffer): any {
  const parseStart = Date.now();
  console.log(
    `[parseLargeJson] Starting chunked parse of ${(buf.length / 1024 / 1024).toFixed(1)}MB buffer`,
  );

  // Envelope: jsonrpc and id are always in the first 200 bytes
  const header = buf.toString('utf8', 0, Math.min(200, buf.length));
  const jsonrpc = header.match(/"jsonrpc":"([^"]+)"/)?.[1] || '2.0';
  const idMatch = header.match(/"id":(-?[0-9]+)/);
  const id = idMatch ? parseInt(idMatch[1]) : -1;

  const parsed: any = { jsonrpc, id, result: {} as any };

  // Find where "result":{ starts for scalar field extraction
  const resultKeyBuf = Buffer.from('"result":{');
  const resultStart = buf.indexOf(resultKeyBuf);
  if (resultStart === -1) return parsed;

  for (const field of ALL_RESULT_FIELDS) {
    const fieldStart = Date.now();

    // Try as chunked array first (handles large arrays without 512MB limit)
    try {
      const boundaries: number[][] = simdjson.findChunkBoundaries(
        buf,
        `result.${field}`,
        MAX_CHUNK_BYTES,
      );
      let arr: any[] = [];
      for (const [s, e] of boundaries) {
        arr = arr.concat(
          JSON.parse('[' + buf.toString('utf8', s, e) + ']'),
        );
      }
      parsed.result[field] = arr;
      console.log(
        `[parseLargeJson] ${field}: ${arr.length} items (${boundaries.length} chunks) in ${Date.now() - fieldStart}ms`,
      );
      continue;
    } catch {
      // Not an array or not found — try as scalar
    }

    const val = extractScalarFromBuffer(buf, field, resultStart);
    if (val !== undefined) parsed.result[field] = val;
  }

  console.log(
    `[parseLargeJson] TOTAL: ${Date.now() - parseStart}ms`,
  );
  return parsed;
}

function utc(): string {
  return new Date().toISOString();
}

function formatSize(bytes: number): string {
  if (bytes < 1024) return `${bytes}B`;
  if (bytes < 1024 * 1024) return `${(bytes / 1024).toFixed(0)}KB`;
  return `${(bytes / 1024 / 1024).toFixed(1)}MB`;
}

export async function streamHttpRequest(
  connection: AxiosInstance,
  request?: any,
): Promise<any> {
  let abortController: AbortController | undefined;
  let timeoutId: ReturnType<typeof setTimeout> | undefined;
  let streamBytesReceived = 0;
  let timedOut = false;

  const method = request?.method || 'unknown';
  const params = request?.params || {};
  const height = params?.height || params?.path || '';
  const reqLabel = height ? `${method}@${height}` : method;

  if (httpTimeout) {
    abortController = new AbortController();

    timeoutId = setTimeout(() => {
      timedOut = true;
      console.log(
        `[HttpClient] [${utc()}] TIMEOUT: ${reqLabel} after ${httpTimeout}ms (received ${formatSize(streamBytesReceived)} so far)`,
      );
      abortController!.abort();
    }, httpTimeout);
  }

  const startTime = Date.now();

  let response: any;
  try {
    response = await connection.post('/', request, {
      responseType: 'stream',
      signal: abortController?.signal,
    });
  } catch (err: any) {
    if (timeoutId) clearTimeout(timeoutId);
    const elapsed = Date.now() - startTime;
    const reason = timedOut
      ? `CLIENT_TIMEOUT after ${httpTimeout}ms`
      : err.code || err.message;
    console.log(
      `[HttpClient] [${utc()}] FETCH_ERROR: ${reqLabel} after ${elapsed}ms — ${reason}`,
    );
    throw err;
  }

  const fetchTime = Date.now() - startTime;
  const statusCode = response.status;
  console.log(
    `[HttpClient] [${utc()}] RESPONSE: ${reqLabel} status=${statusCode} in ${fetchTime}ms, streaming...`,
  );

  return new Promise((resolve, reject) => {
    const bufferChunks: Buffer[] = [];
    let totalLength = 0;
    response.data.on('data', (chunk: Buffer) => {
      bufferChunks.push(chunk);
      totalLength += chunk.length;
      streamBytesReceived = totalLength;
    });
    response.data.on('error', (err: Error) => {
      if (timeoutId) clearTimeout(timeoutId);
      const elapsed = Date.now() - startTime;
      const reason = timedOut
        ? `CLIENT_TIMEOUT after ${httpTimeout}ms`
        : err.message;
      console.log(
        `[HttpClient] [${utc()}] STREAM_ERROR: ${reqLabel} after ${elapsed}ms (received ${formatSize(streamBytesReceived)}) — ${reason}`,
      );
      reject(err);
    });
    response.data.on('end', () => {
      try {
        const streamEndTime = Date.now();
        const bodyTime = streamEndTime - startTime - fetchTime;

        const concatStart = Date.now();
        const buf = Buffer.allocUnsafe(totalLength);
        let offset = 0;
        for (const chunk of bufferChunks) {
          chunk.copy(buf, offset);
          offset += chunk.length;
        }
        const concatTime = Date.now() - concatStart;

        const parseStart = Date.now();
        let result: any;
        if (buf.length < 500 * 1024 * 1024) {
          result = JSON.parse(buf.toString());
        } else {
          result = parseLargeJsonBuffer(buf);
        }
        const parseTime = Date.now() - parseStart;

        if (timeoutId) clearTimeout(timeoutId);
        const totalTime = Date.now() - startTime;
        console.log(
          `[HttpClient] [${utc()}] DONE: ${reqLabel} total=${totalTime}ms (headers=${fetchTime}ms body=${bodyTime}ms concat=${concatTime}ms parse=${parseTime}ms) size=${formatSize(buf.length)} chunks=${bufferChunks.length}`,
        );
        resolve(result);
      } catch (err: any) {
        if (timeoutId) clearTimeout(timeoutId);
        const totalTime = Date.now() - startTime;
        console.log(
          `[HttpClient] [${utc()}] PARSE_ERROR: ${reqLabel} after ${totalTime}ms (size: ${formatSize(streamBytesReceived)}) — ${err.message}`,
        );
        reject(err);
      }
    });
  });
}

export async function httpRequest(
  connection: AxiosInstance,
  request?: any,
): Promise<any> {
  const { data } = await connection.post('/', request);

  return data;
}

export class HttpClient implements RpcClient {
  protected readonly url: string;
  protected readonly headers: Record<string, string>;
  connection: AxiosInstance;

  constructor(endpoint: string | HttpEndpoint) {
    if (typeof endpoint === 'string') {
      // accept host.name:port and assume http protocol
      this.url = hasProtocol(endpoint) ? endpoint : `http://${endpoint}`;
      this.headers = {};
    } else {
      this.url = endpoint.url;
      this.headers = endpoint.headers;
    }

    const { searchParams } = new URL(this.url);

    // Support OnFinality api keys
    const apiKey = searchParams.get('apikey');
    if (apiKey) {
      this.headers.apikey = apiKey;
      this.url = this.url.slice(0, this.url.indexOf('?apikey'));
    }

    const httpAgent = new http.Agent({ keepAlive: true, maxSockets: 10 });
    const httpsAgent = new https.Agent({ keepAlive: true, maxSockets: 10 });

    this.connection = axios.create({
      httpAgent,
      httpsAgent,
      baseURL: this.url,
      headers: {
        'Accept-Encoding': 'gzip,deflate',
        ...(this.headers || {}),
      },
      decompress: true, // Ensure Axios handles decompression
      responseType: 'stream', // Standardize stream handling
      maxContentLength: Infinity, // Allow unlimited response size
      maxBodyLength: Infinity, // Allow unlimited response size
      timeout: httpTimeout, // HTTP request timeout in milliseconds
    });

    console.log(
      `[HttpClient] Initialized with timeout: ${
        httpTimeout || 'none'
      }ms, endpoint: ${this.url}`,
    );
  }

  disconnect(): void {
    // nothing to be done
  }

  async execute(request: JsonRpcRequest): Promise<JsonRpcSuccessResponse> {
    try {
      const rawResponse = await streamHttpRequest(this.connection, request);

      const response = parseJsonRpcResponse(rawResponse);
      if (isJsonRpcErrorResponse(response)) {
        throw new Error(JSON.stringify(response.error));
      }
      return response;
    } catch (err) {
      if (err instanceof Error) {
        throw new Error(`Failed to parse JSON RPC response: ${err.message}`);
      } else {
        throw new Error(`Failed to parse JSON RPC response: ${err}`);
      }
    }
  }
}
