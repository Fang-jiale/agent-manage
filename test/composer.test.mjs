import { test } from 'node:test';
import assert from 'node:assert/strict';
import { draftKey, isCompositionKey, DraftStore } from '../static/composer.js';

function memoryPersistence() {
    const disk = new Map();
    return {
        read: async key => structuredClone(disk.get(key)),
        write: async (key, value) => { disk.set(key, structuredClone(value)); },
        delete: async key => { disk.delete(key); }
    };
}

test('drafts isolate accounts, agents and conversations, including attachments after reload', async () => {
    const persistence = memoryPersistence();
    const store = new DraftStore(persistence);
    const keys = [draftKey('alice', 'agent-a', 'one'), draftKey('alice', 'agent-a', 'two'),
        draftKey('alice', 'agent-b', 'one'), draftKey('bob', 'agent-a', 'one')];
    const attachment = { name: 'notes.txt', dataUrl: 'data:text/plain;base64,aGVsbG8=' };
    keys.forEach((key, index) => store.write(key, { text: `draft ${index}`, files: [attachment] }));
    await store.queue;
    const reloaded = new DraftStore(persistence);
    for (const [index, key] of keys.entries()) {
        assert.deepEqual(await reloaded.read(key), { text: `draft ${index}`, files: [attachment] });
    }
    assert.equal(draftKey(null, 'agent-a', 'one'), null);
    assert.equal(draftKey('alice', 'agent-a', null), null);
});

test('late storage reads cannot overwrite edited or deleted drafts', async () => {
    let resolveRead;
    const persistence = { ...memoryPersistence(), read: () => new Promise(resolve => { resolveRead = resolve; }) };
    const store = new DraftStore(persistence);
    const pending = store.read('one');
    store.write('one', { text: 'newer edit', files: [] });
    resolveRead({ text: 'old disk content', files: [] });
    assert.equal((await pending).text, 'newer edit');
    const pendingDelete = store.read('two');
    store.delete('two');
    resolveRead({ text: 'deleted content', files: [] });
    assert.deepEqual(await pendingDelete, { text: '', files: [] });
});

test('sending one draft preserves the other and does not resurrect the sent draft', async () => {
    const persistence = memoryPersistence();
    const store = new DraftStore(persistence);
    const files = [{ name: 'original.txt' }];
    store.write('one', { text: 'sending', files });
    files.push({ name: 'later.txt' });
    assert.equal((await store.read('one')).files.length, 1);
    store.write('two', { text: 'keep this', files: [] });
    store.delete('one');
    await store.queue;
    const reloaded = new DraftStore(persistence);
    assert.deepEqual(await reloaded.read('one'), { text: '', files: [] });
    assert.equal((await reloaded.read('two')).text, 'keep this');
});

test('unavailable disk storage preserves in-page edits and reports the failure', async () => {
    const errors = [];
    const store = new DraftStore({ read: async () => { throw Error('blocked'); },
        write: async () => { throw Error('quota'); }, delete: async () => {} }, e => errors.push(e.message));
    await store.read('one');
    store.write('one', { text: 'still here', files: [] });
    await store.queue;
    assert.equal((await store.read('one')).text, 'still here');
    assert.deepEqual(errors, ['blocked', 'quota']);
});

test('Chinese candidate confirmation is excluded from send and command selection keys', () => {
    assert.equal(isCompositionKey({ key: 'Enter', isComposing: true, keyCode: 13 }), true);
    assert.equal(isCompositionKey({ key: 'Enter', isComposing: false, keyCode: 229 }), true);
    assert.equal(isCompositionKey({ key: 'Enter', isComposing: false, keyCode: 13 }), false);
});
