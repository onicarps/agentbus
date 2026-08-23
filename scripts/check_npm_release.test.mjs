import assert from "node:assert/strict";
import test from "node:test";

import { evaluateRelease } from "./check_npm_release.mjs";

test("absent package requires publish", () => {
  assert.equal(evaluateRelease("sha512-local", null), true);
});

test("identical package is a safe no-op", () => {
  assert.equal(evaluateRelease("sha512-same", "sha512-same"), false);
});

test("different package fails closed", () => {
  assert.throws(
    () => evaluateRelease("sha512-local", "sha512-remote"),
    /integrity differs/,
  );
});
