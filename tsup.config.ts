import { defineConfig } from 'tsup';

export default defineConfig({
  entry: ['src/index.ts'],
  // Consumers span both module systems: leaselock-ingestion and ll-private are CommonJS,
  // ingestdb's admin_ui is ESM. Ship both rather than forcing an interop shim on either.
  format: ['cjs', 'esm'],
  dts: true,
  sourcemap: true,
  clean: true,
  treeshake: true,
  platform: 'node',
  target: 'node24',
  // winston-transport is a peer dependency: bundling it would give the consumer a second
  // Transport base class, and winston's instanceof checks would stop recognising this one.
  external: ['winston-transport'],
});
