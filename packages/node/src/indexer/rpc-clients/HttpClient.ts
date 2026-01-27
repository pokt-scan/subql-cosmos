// // Copyright 2020-2025 SubQuery Pte Ltd authors & contributors
// SPDX-License-Identifier: GPL-3.0

import http from "http";
import https from "https";
import {
  isJsonRpcErrorResponse,
  JsonRpcRequest,
  JsonRpcSuccessResponse,
  parseJsonRpcResponse,
} from "@cosmjs/json-rpc";
import { HttpEndpoint } from "@cosmjs/tendermint-rpc";
import axios, { AxiosInstance } from "axios";
import { parser } from "stream-json";
import Assembler from "stream-json/Assembler";

import { RpcClient } from "./RpcClient";

// Read HTTP timeout from environment variable, undefined if not set
const httpTimeout = process.env.HTTP_CLIENT_TIMEOUT
  ? parseInt(process.env.HTTP_CLIENT_TIMEOUT, 10)
  : undefined;

export function hasProtocol(url: string): boolean {
  return url.search("://") !== -1;
}

export async function streamHttpRequest(
  connection: AxiosInstance,
  request?: any,
): Promise<any> {
  let abortController: AbortController | undefined;

  if (httpTimeout) {
    abortController = new AbortController();

    setTimeout(() => {
      console.log(`[HttpClient] Canceling request due to timeout (${httpTimeout}ms)`);
      abortController!.abort();
    }, httpTimeout);
  }

  const startTime = Date.now();
  const method = request?.method || 'unknown';

  console.log(`[HttpClient] Starting request: ${method}`);

  const response = await connection.post("/", request, {
    responseType: "stream", // Stream the response to handle large JSON
    signal: abortController?.signal,
  });

  const fetchTime = Date.now() - startTime;
  console.log(`[HttpClient] Response received for ${method} in ${fetchTime}ms, parsing stream...`);

  return new Promise((resolve, reject) => {
    const jsonStream = response.data.pipe(parser());
    jsonStream.on("error", (err: Error) => {
      const totalTime = Date.now() - startTime;
      console.log(`[HttpClient] Stream parse error for ${method} after ${totalTime}ms: ${err.message}`);
      reject(err);
    });
    const asm = Assembler.connectTo(jsonStream);
    asm.on("done", (asm: any) => {
      const totalTime = Date.now() - startTime;
      console.log(`[HttpClient] Request ${method} completed in ${totalTime}ms (fetch: ${fetchTime}ms, parse: ${totalTime - fetchTime}ms)`);
      resolve(asm.current);
    });
  });
}

export async function httpRequest(
  connection: AxiosInstance,
  request?: any,
): Promise<any> {
  const { data } = await connection.post("/", request);

  return data;
}


export class HttpClient implements RpcClient {
  protected readonly url: string;
  protected readonly headers: Record<string, string>;
  connection: AxiosInstance;

  constructor(endpoint: string | HttpEndpoint) {
    if (typeof endpoint === "string") {
      // accept host.name:port and assume http protocol
      this.url = hasProtocol(endpoint) ? endpoint : `http://${endpoint}`;
      this.headers = {};
    } else {
      this.url = endpoint.url;
      this.headers = endpoint.headers;
    }

    const { searchParams } = new URL(this.url);

    // Support OnFinality api keys
    const apiKey = searchParams.get("apikey");
    if (apiKey) {
      this.headers.apikey = apiKey;
      this.url = this.url.slice(0, this.url.indexOf("?apikey"));
    }

    const httpAgent = new http.Agent({ keepAlive: true, maxSockets: 10 });
    const httpsAgent = new https.Agent({ keepAlive: true, maxSockets: 10 });

    this.connection = axios.create({
      httpAgent,
      httpsAgent,
      baseURL: this.url,
      headers: {
        "Accept-Encoding": "gzip,deflate",
        ...(this.headers || {}),
      },
      decompress: true, // Ensure Axios handles decompression
      responseType: "stream", // Standardize stream handling
      maxContentLength: Infinity, // Allow unlimited response size
      maxBodyLength: Infinity,   // Allow unlimited response size
      timeout: httpTimeout, // HTTP request timeout in milliseconds
    });

    console.log(`[HttpClient] Initialized with timeout: ${httpTimeout || 'none'}ms, endpoint: ${this.url}`);
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
