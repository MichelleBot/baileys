import { encodeNode } from 'whatsapp-rust-bridge'

export const encodeBinaryNode = (node) => {
	const bytesWritten = encodeNode(node)
    return bytesWritten
}