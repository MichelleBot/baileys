import WebSocket from 'ws';
import { DEFAULT_ORIGIN } from '../../Defaults/index.js';
import { AbstractSocketClient } from './types.js';
export class WebSocketClient extends AbstractSocketClient {
    constructor() {
        super(...arguments);
        this.socket = null;
    }
    get isOpen() {
        return this.socket?.readyState === WebSocket.OPEN;
    }
    get isClosed() {
        return this.socket === null || this.socket?.readyState === WebSocket.CLOSED;
    }
    get isClosing() {
        return this.socket === null || this.socket?.readyState === WebSocket.CLOSING;
    }
    get isConnecting() {
        return this.socket?.readyState === WebSocket.CONNECTING;
    }
    connect() {
        if (this.socket) {
            return;
        }
        this.socket = new WebSocket(this.url, {
            origin: DEFAULT_ORIGIN,
            headers: this.config.options?.headers,
            handshakeTimeout: this.config.connectTimeoutMs,
            timeout: this.config.connectTimeoutMs,
            agent: this.config.agent
        });
        this.socket.setMaxListeners(0);
        const events = ['close', 'error', 'upgrade', 'message', 'open', 'ping', 'pong', 'unexpected-response'];
        for (const event of events) {
            this.socket?.on(event, (...args) => this.emit(event, ...args));
        }
    }
    async close() {
        if (!this.socket) {
            return;
        }
        if (this.socket.readyState === WebSocket.CLOSED) {
            this.socket = null;
            return;
        }
        const ws = this.socket;
        const closePromise = new Promise(resolve => {
            const timeout = setTimeout(() => {
                try {
                    ws.terminate();
                } catch { }
                resolve();
            }, 5000);
            ws.once('close', () => {
                clearTimeout(timeout);
                resolve();
            });
        });
        try {
            ws.close();
        } catch {
            try {
                ws.terminate();
            } catch { }
        }
        await closePromise;
        this.socket = null;
    }
    send(str, cb) {
        if (!this.isOpen) {
            cb?.(new Error('WebSocket is not open'));
            return false;
        }
        try {
            this.socket?.send(str, cb);
            return Boolean(this.socket);
        } catch (err) {
            cb?.(err);
            return false;
        }
    }
}
//# sourceMappingURL=websocket.js.map