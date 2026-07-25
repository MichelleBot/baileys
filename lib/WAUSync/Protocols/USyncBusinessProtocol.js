import { assertNodeErrorFree } from '../../WABinary/index.js';
export class USyncBusinessProtocol {
    constructor() {
        this.name = 'business';
    }
    getQueryElement() {
        return {
            tag: 'business',
            attrs: {},
            content: [{ tag: 'verified_name', attrs: {} }]
        };
    }
    getUserElement(user) {
        void user;
        return null;
    }
    parser(node) {
        if (node.tag === 'business') {
            assertNodeErrorFree(node);
            const verifiedName = node.content?.find(c => c.tag === 'verified_name');
            return verifiedName ? verifiedName.content : null;
        }
        return null;
    }
}
