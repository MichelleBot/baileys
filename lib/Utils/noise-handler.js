import { Boom } from '@hapi/boom';
import { Mutex } from 'async-mutex';
import { proto } from '../../WAProto/index.js';
import { NOISE_MODE, WA_CERT_DETAILS } from '../Defaults/index.js';
import { decodeBinaryNode } from '../WABinary/index.js';
import { aesDecryptGCM, aesEncryptGCM, Curve, hkdf, sha256 } from './crypto.js';
const IV_LENGTH = 12;
const EMPTY_BUFFER = Buffer.alloc(0);
/**
 * Builds a fresh AES-GCM IV from the counter on every call. Stage 7 (M10):
 * the previous implementation reused a single shared `Uint8Array` and mutated
 * its bytes in place. Safe only because `aesEncryptGCM` is synchronous — any
 * future move to an async/streaming AEAD silently reuses the IV with the same
 * key, which is catastrophic for AES-GCM. The fresh allocation costs ~12
 * bytes per call and removes the implicit "must stay sync" invariant.
 *
 * Single source of truth for handshake AND transport IV construction (the
 * earlier `generateIV` ArrayBuffer+DataView variant was identical in output
 * — 12 zero bytes with the counter written as big-endian uint32 at offset
 * 8 — but a redundant second implementation; consolidating prevents the
 * two from drifting apart in future edits).
 */
const ivForCounter = (counter) => {
    const iv = new Uint8Array(IV_LENGTH);
    iv[8] = (counter >>> 24) & 0xff;
    iv[9] = (counter >>> 16) & 0xff;
    iv[10] = (counter >>> 8) & 0xff;
    iv[11] = counter & 0xff;
    return iv;
};
/**
 * Test-only export of {@link ivForCounter}. Not part of the public API
 * surface — the leading underscore + `__testOnly_` prefix flag that.
 *
 * The M10 regression test uses this to assert that consecutive calls
 * return distinct `Uint8Array` instances. A ciphertext-inequality check
 * alone passes even when the IV buffer is shared and merely mutated
 * between calls (counter still advances → ciphertexts differ); the
 * identity check pins the "fresh allocation per call" contract directly.
 */
export const __testOnly_ivForCounter = ivForCounter;
class TransportState {
    constructor(encKey, decKey) {
        this.encKey = encKey;
        this.decKey = decKey;
        this.readCounter = 0;
        this.writeCounter = 0;
    }
    encrypt(plaintext) {
        const c = this.writeCounter++;
        return aesEncryptGCM(plaintext, this.encKey, ivForCounter(c), EMPTY_BUFFER);
    }
    decrypt(ciphertext) {
        const c = this.readCounter++;
        return aesDecryptGCM(ciphertext, this.decKey, ivForCounter(c), EMPTY_BUFFER);
    }
}
export const makeNoiseHandler = ({ keyPair: { private: privateKey, public: publicKey }, NOISE_HEADER, logger, routingInfo }) => {
    logger = logger.child({ class: 'ns' });
    const data = Buffer.from(NOISE_MODE);
    let hash = data.byteLength === 32 ? data : sha256(data);
    let salt = hash;
    let encKey = hash;
    let decKey = hash;
    let counter = 0;
    let sentIntro = false;
    let inBytes = Buffer.alloc(0);
    /**
     * Serializes `decodeFrame` so concurrent socket reads don't interleave on
     * the shared `inBytes` buffer and the awaited `processData` loop (M10).
     * Critical because `processData` awaits `decodeBinaryNode` in transport
     * mode — a second `decodeFrame` invocation that arrives mid-await would
     * otherwise see a half-drained `inBytes` and either lose frames or
     * de-frame across two distinct payloads.
     */
    const decodeFrameMutex = new Mutex();
    let transport = null;
    let isWaitingForTransport = false;
    let pendingOnFrame = null;
    let introHeader;
    if (routingInfo) {
        introHeader = Buffer.alloc(7 + routingInfo.byteLength + NOISE_HEADER.length);
        introHeader.write('ED', 0, 'utf8');
        introHeader.writeUint8(0, 2);
        introHeader.writeUint8(1, 3);
        introHeader.writeUint8(routingInfo.byteLength >> 16, 4);
        introHeader.writeUint16BE(routingInfo.byteLength & 65535, 5);
        introHeader.set(routingInfo, 7);
        introHeader.set(NOISE_HEADER, 7 + routingInfo.byteLength);
    }
    else {
        introHeader = Buffer.from(NOISE_HEADER);
    }
    const authenticate = (data) => {
        if (!transport) {
            hash = sha256(Buffer.concat([hash, data]));
        }
    };
    const encrypt = (plaintext) => {
        if (transport) {
            return transport.encrypt(plaintext);
        }
        const result = aesEncryptGCM(plaintext, encKey, ivForCounter(counter++), hash);
        authenticate(result);
        return result;
    };
    const decrypt = (ciphertext) => {
        if (transport) {
            return transport.decrypt(ciphertext);
        }
        const result = aesDecryptGCM(ciphertext, decKey, ivForCounter(counter++), hash);
        authenticate(ciphertext);
        return result;
    };
    const localHKDF = (data) => {
        const key = hkdf(Buffer.from(data), 64, { salt, info: '' });
        return [key.subarray(0, 32), key.subarray(32)];
    };
    const mixIntoKey = (data) => {
        const [write, read] = localHKDF(data);
        salt = write;
        encKey = read;
        decKey = read;
        counter = 0;
    };
    const finishInit = async () => {
        isWaitingForTransport = true;
        const [write, read] = localHKDF(new Uint8Array(0));
        transport = new TransportState(write, read);
        isWaitingForTransport = false;
        logger.trace('Noise handler transitioned to Transport state');
        if (pendingOnFrame) {
            // The pending-frame drain runs the SAME `processData` loop that
            // `decodeFrame` does and mutates the SAME `inBytes` buffer. We
            // must acquire the decodeFrame mutex so a `decodeFrame` arriving
            // concurrently with `finishInit` queues behind us instead of
            // racing the transport-mode async path through `inBytes`.
            await decodeFrameMutex.runExclusive(async () => {
                logger.trace({ length: inBytes.length }, 'Flushing buffered frames after transport ready');
                await processData(pendingOnFrame);
                pendingOnFrame = null;
            });
        }
    };
    const processData = async (onFrame) => {
        let size;
        while (true) {
            if (inBytes.length < 3)
                return;
            size = (inBytes[0] << 16) | (inBytes[1] << 8) | inBytes[2];
            if (inBytes.length < size + 3)
                return;
            let frame = inBytes.subarray(3, size + 3);
            inBytes = inBytes.subarray(size + 3);
            if (transport) {
                const result = transport.decrypt(frame);
                frame = await decodeBinaryNode(result);
            }
            if (logger.level === 'trace') {
                logger.trace({ msg: frame?.attrs?.id }, 'recv frame');
            }
            onFrame(frame);
        }
    };
    authenticate(NOISE_HEADER);
    authenticate(publicKey);
    return {
        encrypt,
        decrypt,
        authenticate,
        mixIntoKey,
        finishInit,
        processHandshake: ({ serverHello }, noiseKey) => {
            authenticate(serverHello.ephemeral);
            mixIntoKey(Curve.sharedKey(privateKey, serverHello.ephemeral));
            const decStaticContent = decrypt(serverHello.static);
            mixIntoKey(Curve.sharedKey(privateKey, decStaticContent));
            const certDecoded = decrypt(serverHello.payload);
            const { intermediate: certIntermediate, leaf } = proto.CertChain.decode(certDecoded);
            // leaf
            if (!leaf?.details || !leaf?.signature) {
                throw new Boom('invalid noise leaf certificate', { statusCode: 400 });
            }
            if (!certIntermediate?.details || !certIntermediate?.signature) {
                throw new Boom('invalid noise intermediate certificate', { statusCode: 400 });
            }
            const details = proto.CertChain.NoiseCertificate.Details.decode(certIntermediate.details);
            const { issuerSerial } = details;
            const verify = Curve.verify(details.key, leaf.details, leaf.signature);
            const verifyIntermediate = Curve.verify(WA_CERT_DETAILS.PUBLIC_KEY, certIntermediate.details, certIntermediate.signature);
            if (!verify) {
                throw new Boom('noise certificate signature invalid', { statusCode: 400 });
            }
            if (!verifyIntermediate) {
                throw new Boom('noise intermediate certificate signature invalid', { statusCode: 400 });
            }
            if (issuerSerial !== WA_CERT_DETAILS.SERIAL) {
                throw new Boom('certification match failed', { statusCode: 400 });
            }
            const keyEnc = encrypt(noiseKey.public);
            mixIntoKey(Curve.sharedKey(noiseKey.private, serverHello.ephemeral));
            return keyEnc;
        },
        encodeFrame: (data) => {
            if (transport) {
                data = transport.encrypt(data);
            }
            const dataLen = data.byteLength;
            const introSize = sentIntro ? 0 : introHeader.length;
            const frame = Buffer.allocUnsafe(introSize + 3 + dataLen);
            if (!sentIntro) {
                frame.set(introHeader);
                sentIntro = true;
            }
            frame[introSize] = (dataLen >>> 16) & 0xff;
            frame[introSize + 1] = (dataLen >>> 8) & 0xff;
            frame[introSize + 2] = dataLen & 0xff;
            frame.set(data, introSize + 3);
            return frame;
        },
        decodeFrame: (newData, onFrame) => {
            // M10: serialize the inBytes mutation + processData drain.
            return decodeFrameMutex.runExclusive(async () => {
                if (isWaitingForTransport) {
                    inBytes = Buffer.concat([inBytes, newData]);
                    pendingOnFrame = onFrame;
                    return;
                }
                if (inBytes.length === 0) {
                    inBytes = Buffer.from(newData);
                }
                else {
                    inBytes = Buffer.concat([inBytes, newData]);
                }
                await processData(onFrame);
            });
        }
    };
};
//# sourceMappingURL=noise-handler.js.map