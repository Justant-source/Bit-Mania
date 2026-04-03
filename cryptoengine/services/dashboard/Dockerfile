FROM node:20-alpine AS builder

WORKDIR /app

COPY services/dashboard/package.json services/dashboard/package-lock.json* ./
RUN npm install

COPY services/dashboard/tsconfig.json ./
COPY services/dashboard/src/ ./src/

RUN npm run build

# --- Production stage ---
FROM node:20-alpine

WORKDIR /app

COPY --from=builder /app/package.json ./
COPY --from=builder /app/node_modules ./node_modules
COPY --from=builder /app/dist ./dist

EXPOSE 3000 3001

CMD ["node", "dist/server.js"]
