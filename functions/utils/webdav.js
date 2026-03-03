function normalizeBaseUrl(raw) {
  if (!raw) return '';
  try {
    return new URL(String(raw)).toString().replace(/\/+$/, '');
  } catch {
    return '';
  }
}

function normalizeToken(value) {
  if (!value) return '';
  return String(value).replace(/^Bearer\s+/i, '').trim();
}

function normalizePath(value) {
  const normalized = String(value || '')
    .replace(/\\/g, '/')
    .trim();

  const output = [];
  for (const part of normalized.split('/')) {
    const piece = part.trim();
    if (!piece || piece === '.') continue;
    if (piece === '..') {
      output.pop();
      continue;
    }
    output.push(piece);
  }
  return output.join('/');
}

function splitPath(value) {
  const normalized = normalizePath(value);
  if (!normalized) return [];
  return normalized.split('/').filter(Boolean);
}

function encodeSegments(segments) {
  if (!segments.length) return '';
  return segments.map((segment) => encodeURIComponent(segment)).join('/');
}

function authMode(config) {
  if (config.bearerToken) return 'bearer';
  if (config.username && config.password) return 'basic';
  return 'none';
}

export function getWebDAVConfig(env = {}) {
  return {
    baseUrl: normalizeBaseUrl(env.WEBDAV_BASE_URL),
    username: String(env.WEBDAV_USERNAME || '').trim(),
    password: String(env.WEBDAV_PASSWORD || ''),
    bearerToken: normalizeToken(env.WEBDAV_BEARER_TOKEN || env.WEBDAV_TOKEN || ''),
    rootPath: normalizePath(env.WEBDAV_ROOT_PATH || ''),
  };
}

export function hasWebDAVConfig(env = {}) {
  const config = getWebDAVConfig(env);
  return Boolean(config.baseUrl) && authMode(config) !== 'none';
}

function buildAuthHeaders(config, extra = {}) {
  const headers = { ...extra };
  const mode = authMode(config);
  if (mode === 'bearer') {
    headers.Authorization = `Bearer ${config.bearerToken}`;
  } else if (mode === 'basic') {
    const encoded = btoa(`${config.username}:${config.password}`);
    headers.Authorization = `Basic ${encoded}`;
  }
  return headers;
}

function buildStoragePath(config, storagePath = '') {
  const allSegments = [...splitPath(config.rootPath), ...splitPath(storagePath)];
  return encodeSegments(allSegments);
}

function buildUrl(config, storagePath = '') {
  const relative = buildStoragePath(config, storagePath);
  return relative ? `${config.baseUrl}/${relative}` : config.baseUrl;
}

async function decodeErrorTextSafe(response) {
  try {
    const text = await response.text();
    return String(text || '').slice(0, 500);
  } catch {
    return '';
  }
}

async function fetchDav(config, method, storagePath = '', { headers = {}, body = null } = {}) {
  return fetch(buildUrl(config, storagePath), {
    method,
    headers: buildAuthHeaders(config, headers),
    body,
  });
}

async function ensureCollectionPath(config, storagePath) {
  const rootSegments = splitPath(config.rootPath);
  const fileSegments = splitPath(storagePath);
  const directorySegments = [...rootSegments, ...fileSegments.slice(0, -1)];
  if (directorySegments.length === 0) return;

  for (let index = 0; index < directorySegments.length; index += 1) {
    const partial = directorySegments.slice(0, index + 1);
    const response = await fetch(`${config.baseUrl}/${encodeSegments(partial)}`, {
      method: 'MKCOL',
      headers: buildAuthHeaders(config),
    });

    if ([200, 201, 204, 301, 302, 405].includes(response.status)) {
      continue;
    }

    const detail = await decodeErrorTextSafe(response);
    throw new Error(`WebDAV MKCOL failed (${response.status}): ${detail || 'Unknown error'}`);
  }
}

export async function uploadToWebDAV(arrayBuffer, storagePath, contentType, env = {}) {
  const config = getWebDAVConfig(env);
  if (!config.baseUrl) {
    throw new Error('WebDAV base URL is not configured.');
  }
  if (authMode(config) === 'none') {
    throw new Error('WebDAV auth is not configured.');
  }

  await ensureCollectionPath(config, storagePath);

  const response = await fetchDav(config, 'PUT', storagePath, {
    headers: {
      'Content-Type': contentType || 'application/octet-stream',
      'Content-Length': String(arrayBuffer.byteLength || 0),
    },
    body: arrayBuffer,
  });

  if (!response.ok && ![201, 204].includes(response.status)) {
    const detail = await decodeErrorTextSafe(response);
    throw new Error(`WebDAV upload failed (${response.status}): ${detail || 'Unknown error'}`);
  }

  return {
    path: normalizePath(storagePath),
    etag: response.headers.get('etag') || null,
  };
}

export async function getWebDAVFile(storagePath, env = {}, options = {}) {
  const config = getWebDAVConfig(env);
  if (!config.baseUrl) {
    throw new Error('WebDAV base URL is not configured.');
  }
  if (authMode(config) === 'none') {
    throw new Error('WebDAV auth is not configured.');
  }

  const headers = {};
  if (options.range) {
    headers.Range = options.range;
  }

  const response = await fetchDav(config, 'GET', storagePath, { headers });
  if (!response.ok && response.status !== 206) {
    if (response.status === 404) return null;
    const detail = await decodeErrorTextSafe(response);
    throw new Error(`WebDAV download failed (${response.status}): ${detail || 'Unknown error'}`);
  }
  return response;
}

export async function deleteWebDAVFile(storagePath, env = {}) {
  const config = getWebDAVConfig(env);
  if (!config.baseUrl) return false;
  if (authMode(config) === 'none') return false;

  const response = await fetchDav(config, 'DELETE', storagePath);
  if (response.ok || response.status === 404) return true;

  const detail = await decodeErrorTextSafe(response);
  throw new Error(`WebDAV delete failed (${response.status}): ${detail || 'Unknown error'}`);
}

export async function checkWebDAVConnection(env = {}) {
  if (!hasWebDAVConfig(env)) {
    return {
      connected: false,
      configured: false,
      message: 'Not configured',
    };
  }

  const config = getWebDAVConfig(env);
  try {
    const optionsResponse = await fetchDav(config, 'OPTIONS', '', {
      headers: { Depth: '0' },
    });

    if (optionsResponse.ok) {
      return {
        connected: true,
        configured: true,
        status: optionsResponse.status,
        message: 'Connected',
      };
    }

    const propfindBody = [
      '<?xml version="1.0" encoding="utf-8" ?>',
      '<d:propfind xmlns:d="DAV:">',
      '  <d:prop><d:displayname /></d:prop>',
      '</d:propfind>',
    ].join('');

    const propfindResponse = await fetchDav(config, 'PROPFIND', '', {
      headers: {
        Depth: '0',
        'Content-Type': 'application/xml; charset=utf-8',
      },
      body: propfindBody,
    });

    const connected = propfindResponse.ok || propfindResponse.status === 207;
    const detail = connected ? '' : await decodeErrorTextSafe(propfindResponse);
    return {
      connected,
      configured: true,
      status: propfindResponse.status,
      message: connected ? 'Connected' : (detail || 'Connection failed'),
      detail: detail || undefined,
    };
  } catch (error) {
    return {
      connected: false,
      configured: true,
      message: error.message || 'Connection failed',
      detail: error.message || 'Connection failed',
    };
  }
}

export function normalizeWebDAVPath(value = '') {
  return normalizePath(value);
}
