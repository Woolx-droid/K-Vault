const STORAGE_PREFIXES = ['img:', 'vid:', 'aud:', 'doc:', 'r2:', 's3:', 'discord:', 'hf:', 'webdav:', 'github:', ''];

function normalizeFolderPath(value = '') {
  const raw = String(value || '').replace(/\\/g, '/').trim();
  const output = [];
  for (const part of raw.split('/')) {
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

async function getRecordWithKey(env, fileId) {
  const hasKnownPrefix = STORAGE_PREFIXES.some((prefix) => prefix && fileId.startsWith(prefix));
  const candidateKeys = hasKnownPrefix ? [fileId] : STORAGE_PREFIXES.map((prefix) => `${prefix}${fileId}`);

  for (const key of candidateKeys) {
    const record = await env.img_url.getWithMetadata(key);
    if (record?.metadata) {
      return { record, kvKey: key };
    }
  }
  return { record: null, kvKey: fileId };
}

export async function onRequestPost(context) {
  const { request, env } = context;

  if (!env.img_url) {
    return jsonResponse({ success: false, error: 'KV binding img_url is not configured.' }, 500);
  }

  const body = await request.json().catch(() => ({}));
  const ids = Array.isArray(body.ids)
    ? body.ids.map((item) => {
      try {
        return decodeURIComponent(String(item || '').trim());
      } catch {
        return String(item || '').trim();
      }
    }).filter(Boolean)
    : [];
  const targetFolderPath = normalizeFolderPath(body.targetFolderPath || body.folderPath || body.path || '');

  if (ids.length === 0) {
    return jsonResponse({ success: false, error: 'ids is required.' }, 400);
  }

  let moved = 0;
  const notFound = [];

  for (const id of ids) {
    const { record, kvKey } = await getRecordWithKey(env, id);
    if (!record?.metadata) {
      notFound.push(id);
      continue;
    }

    const metadata = {
      ...(record.metadata || {}),
      folderPath: targetFolderPath,
    };
    await env.img_url.put(kvKey, '', { metadata });
    moved += 1;
  }

  return jsonResponse({
    success: true,
    targetFolderPath,
    requested: ids.length,
    moved,
    notFound,
  });
}

function jsonResponse(body, status = 200) {
  return new Response(JSON.stringify(body), {
    status,
    headers: { 'Content-Type': 'application/json' },
  });
}
