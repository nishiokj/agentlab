import os
import tempfile

from agentlab_core import ArtifactStore, HashChain, canonical_dumps, sha256_bytes


def test_canonical_json_stable_order():
    obj = {"b": 2, "a": 1}
    assert canonical_dumps(obj) == "{\"a\":1,\"b\":2}"


def test_artifact_store_roundtrip():
    with tempfile.TemporaryDirectory() as tmp:
        store = ArtifactStore(tmp)
        ref = store.put_bytes(b"hello")
        assert ref.startswith("artifact://sha256/")
        assert store.get_bytes(ref) == b"hello"


def test_hashchain_prev_self():
    hc = HashChain()
    line1 = b'{"event":1}'
    h1 = hc.hash_line(line1)
    assert h1 == sha256_bytes(line1)
    assert hc.current_prev() == h1
