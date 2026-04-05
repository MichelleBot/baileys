import { createHmac } from 'node:crypto'
import { getBinaryNodeChild, getBinaryNodeChildren, isLidUser, jidNormalizedUser } from '../WABinary/index.js';
const TC_TOKEN_BUCKET_DURATION = 604800 // 7 days
const TC_TOKEN_NUM_BUCKETS = 4 // ~28-day rolling window

// WA Web has separate sender/receiver AB props for these but they're identical today
export function isTcTokenExpired(timestamp) {
    if (timestamp === null || timestamp === undefined)
        return true;
    const ts = typeof timestamp === 'string' ? parseInt(timestamp) : timestamp;
    if (isNaN(ts))
        return true;
    const now = Math.floor(Date.now() / 1000);
    const currentBucket = Math.floor(now / TC_TOKEN_BUCKET_DURATION);
    const cutoffBucket = currentBucket - (TC_TOKEN_NUM_BUCKETS - 1);
    const cutoffTimestamp = cutoffBucket * TC_TOKEN_BUCKET_DURATION;
    return ts < cutoffTimestamp;
}

export function shouldSendNewTcToken(senderTimestamp) {
    if (senderTimestamp === undefined)
        return true;
    const now = Math.floor(Date.now() / 1000);
    const currentBucket = Math.floor(now / TC_TOKEN_BUCKET_DURATION);
    const senderBucket = Math.floor(senderTimestamp / TC_TOKEN_BUCKET_DURATION);
    return currentBucket > senderBucket;
}
/** Resolve JID to LID for tctoken storage (WA Web stores under LID) */
export async function resolveTcTokenJid(jid, getLIDForPN) {
    if (isLidUser(jid))
        return jid;
    const lid = await getLIDForPN(jid);
    return lid ?? jid;
}

/** Resolve target JID for issuing privacy token based on AB prop 14303 */
export async function resolveIssuanceJid(jid, issueToLid, getLIDForPN, getPNForLID) {
   if (issueToLid) {
      if (isLidUser(jid)) return jid;
      const lid = await getLIDForPN(jid);
      return lid ?? jid;
   }

   if (!isLidUser(jid)) return jid;
   if (getPNForLID) {
      const pn = await getPNForLID(jid);
      return pn ?? jid;
   }

   return jid;
}

export async function buildTcTokenFromJid({ authState, jid, baseContent = [], getLIDForPN }) {
    try {
        const storageJid = getLIDForPN ? await resolveTcTokenJid(jid, getLIDForPN) : jid;
        const tcTokenData = await authState.keys.get('tctoken', [storageJid]);
        const entry = tcTokenData?.[storageJid];
        const tcTokenBuffer = entry?.token;
        if (!tcTokenBuffer?.length || isTcTokenExpired(entry?.timestamp)) {
            if (tcTokenBuffer) {
                await authState.keys.set({ tctoken: { [storageJid]: null } });
            }
            return baseContent.length > 0 ? baseContent : undefined;
        }
        baseContent.push({
            tag: 'tctoken',
            attrs: {},
            content: tcTokenBuffer
        });
        return baseContent;
    }
    catch (error) {
        return baseContent.length > 0 ? baseContent : undefined;
    }
}

export async function storeTcTokensFromIqResult({ result, fallbackJid, keys, getLIDForPN, onNewJidStored }) {
    const tokensNode = getBinaryNodeChild(result, 'tokens');
    if (!tokensNode)
        return;
    const tokenNodes = getBinaryNodeChildren(tokensNode, 'token');
    for (const tokenNode of tokenNodes) {
        if (tokenNode.attrs.type !== 'trusted_contact' || !(tokenNode.content instanceof Uint8Array)) {
            continue;
        }
        // In notifications tokenNode.attrs.jid is your own device JID, not the sender's
		  const rawJid = jidNormalizedUser(fallbackJid || tokenNode.attrs.jid);
        const storageJid = await resolveTcTokenJid(rawJid, getLIDForPN);
        const existingTcData = await keys.get('tctoken', [storageJid]);
        const existingEntry = existingTcData[storageJid];
        const existingTs = existingEntry?.timestamp ? Number(existingEntry.timestamp) : 0;
        const incomingTs = tokenNode.attrs.t ? Number(tokenNode.attrs.t) : 0;
        if (existingTs > 0 && incomingTs > 0 && existingTs > incomingTs) {
            continue;
        }
        // timestamp-less tokens would be immediately expired
        if (existingTs > 0 && !incomingTs) {
            continue;
        }
        await keys.set({
            tctoken: {
                [storageJid]: {
                    ...existingEntry,
                    token: Buffer.from(tokenNode.content),
                    timestamp: tokenNode.attrs.t
                }
            }
        });
        onNewJidStored?.(storageJid);
    }
}