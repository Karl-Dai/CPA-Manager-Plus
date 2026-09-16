import { readFileSync } from 'node:fs';

const fail = (message) => {
  throw new Error(`Runtime 12 Compose validation failed: ${message}`);
};

const document = JSON.parse(readFileSync(0, 'utf8'));
const services = document.services ?? {};
for (const name of ['cpamp-ingress', 'cpamp-manager', 'cpamp-runtime']) {
  if (!services[name]) fail(`missing service ${name}`);
}

const ingress = services['cpamp-ingress'];
const manager = services['cpamp-manager'];
const runtime = services['cpamp-runtime'];
const published = Object.entries(services).flatMap(([service, config]) =>
  (config.ports ?? []).map((port) => ({ service, ...port }))
);
if (
  published.length !== 1 ||
  published[0].service !== 'cpamp-ingress' ||
  Number(published[0].target) !== 18317 ||
  String(published[0].published) !== '18317'
) {
  fail(`expected only cpamp-ingress 18317:18317, got ${JSON.stringify(published)}`);
}

const ingressEnvironment = ingress.environment ?? {};
if (
  ingressEnvironment.CPAMP_MANAGER_URL !== 'http://cpamp-manager:18317' ||
  ingressEnvironment.CPAMP_GATEWAY_URL !== 'http://cpamp-runtime:8317'
) {
  fail('Ingress upstreams must address Manager and Gateway directly');
}
if ((ingress.volumes ?? []).length !== 0) fail('Ingress must not mount product data or secrets');

const volumeTargets = (service) =>
  new Map((service.volumes ?? []).map((volume) => [volume.target, volume]));
const managerVolumes = volumeTargets(manager);
const runtimeVolumes = volumeTargets(runtime);
if (!managerVolumes.has('/data') || managerVolumes.has('/runtime')) {
  fail('Manager storage ownership must remain limited to Manager data');
}
if (!runtimeVolumes.has('/runtime') || runtimeVolumes.has('/data')) {
  fail('Runtime storage ownership must remain limited to Runtime data');
}
const managerSecret = managerVolumes.get('/run/cpamp/runtime-secret');
const runtimeSecret = runtimeVolumes.get('/run/cpamp/runtime-secret');
if (!managerSecret?.read_only || runtimeSecret?.read_only) {
  fail(
    'Runtime transport secret must be read-only for Manager and writable only by Runtime bootstrap'
  );
}
if (managerSecret.source !== runtimeSecret.source) {
  fail('Manager and Runtime must consume the same narrow transport-secret volume');
}

for (const [name, service] of [
  ['cpamp-manager', manager],
  ['cpamp-runtime', runtime],
]) {
  if ((service.ports ?? []).length !== 0) fail(`${name} must not publish host ports`);
  if (service.network_mode === 'host') fail(`${name} must not use host networking`);
}

console.log(
  'Runtime 12 Compose validation passed: one public 18317 mapping and isolated state ownership'
);
