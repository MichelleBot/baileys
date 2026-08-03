import { decodeNode } from 'whatsapp-rust-bridge'

export const decodeBinaryNode = (buffer) => {
	return decodeNode(buffer)
}