FROM node:20-alpine as build-stage

WORKDIR /usr/src/app

COPY package*.json tsconfig.json ./

RUN npm ci

COPY app/ ./app/

RUN npm run build

RUN npm prune --production

FROM node:20-alpine as runner

WORKDIR /usr/src/app

USER node

COPY --chown=node:node package*.json ./
COPY --chown=node:node --from=build-stage /usr/src/app/node_modules ./node_modules
COPY --chown=node:node --from=build-stage /usr/src/app/dist ./dist

ENV NODE_ENV=production

CMD ["node", "dist/server.js"]