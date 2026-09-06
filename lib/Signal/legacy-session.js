/**
 * Auth states written before the WASM backend hold sessions in the JS libsignal
 * shape — `SessionRecord.serialize()` returned a plain object, never bytes:
 *
 *   { _sessions: { [baseKeyB64]: SessionEntry }, version: 'v1' }
 *
 * The bridge can migrate one of these, but it picks `Object.keys(_sessions)[0]`,
 * which is insertion-ordered — after a ratchet rotation that is an old *closed*
 * state, not the live one, so the upgraded session would be unusable. It also
 * accepts a bare `SessionEntry` (anything carrying `registrationId` +
 * `currentRatchet`), so selecting the open entry here hands it the right state.
 */
/** libsignal marks a live session with `closed === -1`. */
const OPEN = -1;
export const isLegacySessionRecord = (value) => typeof value === 'object' && value !== null && !ArrayBuffer.isView(value) && '_sessions' in value;
/** The entry's own id, or the record's for a pre-v1 shape. */
export const entryRegistrationId = (entry, record) => (typeof entry?.registrationId === 'number' ? entry.registrationId : record.registrationId);
const isUsableEntry = (entry, record) => !!entry && typeof entryRegistrationId(entry, record) === 'number' && !!entry.currentRatchet;
/**
 * The live session state, or undefined when every state is closed (or the
 * record is empty). Mirrors libsignal's `getOpenSession()`.
 */
export const pickOpenLegacySession = (record) => {
    for (const entry of Object.values(record._sessions || {})) {
        if (isUsableEntry(entry, record) && entry.indexInfo?.closed === OPEN) {
            return entry;
        }
    }
    return undefined;
};
export const hasOpenLegacySession = (record) => !!pickOpenLegacySession(record);
/** A single state, as handed to the bridge by `loadSession` for legacy records. */
export const isLegacySessionEntry = (value) => typeof value === 'object' &&
    value !== null &&
    !ArrayBuffer.isView(value) &&
    'registrationId' in value &&
    'currentRatchet' in value;
/**
 * `indexInfo.baseKey` is the JS libsignal equivalent of wacore's
 * `alice_base_key`, so the retry protections keep working on a session that has
 * not been rewritten into the bridge format yet.
 */
export const legacySessionInfo = (entry) => {
    const baseKey = entry.indexInfo?.baseKey;
    const { registrationId } = entry;
    if (!baseKey || typeof registrationId !== 'number') {
        return null;
    }
    return { baseKey: new Uint8Array(Buffer.from(baseKey, 'base64')), registrationId };
};
//# sourceMappingURL=legacy-session.js.map