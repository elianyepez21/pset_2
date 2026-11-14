#!/usr/bin/env bash
# Kestra (edición OSS) lee los secretos de variables de entorno SECRET_* codificadas en base64.
# Este script convierte .env en .env_encoded (ambos ignorados por git).
set -euo pipefail
cd "$(dirname "$0")/.."
[ -f .env ] || { echo "Falta .env (copia .env.example y complétalo)"; exit 1; }
: > .env_encoded
while IFS='=' read -r key value; do
  case "$key" in ''|\#*) continue;; esac
  [ -n "$value" ] || continue
  echo "SECRET_${key}=$(printf '%s' "$value" | base64 | tr -d '\n')" >> .env_encoded
done < .env
echo ".env_encoded generado con $(wc -l < .env_encoded) secretos."
