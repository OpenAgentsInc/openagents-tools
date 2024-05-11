# Builder
FROM node:18 as builder

ADD . /app
WORKDIR /app

RUN npm i && npm run build

# Runner
FROM node:18
RUN mkdir -p /app
WORKDIR /app
COPY --from=builder /app/build /app/build
COPY package.json /app
COPY package-lock.json /app
RUN \
npm i --production && \
chown 1000:1000 -Rf /app 

  
ENV POOL_ADDRESS="pool.openagents.com"
ENV POOL_PORT="5000"
ENV POOL_SSL="true"

ENV PRODUCTION="true"
ENV CACHE_PATH="/cache" 
 
VOLUME /cache


EXPOSE 5000
USER 1000
CMD ["npm","run", "start"]