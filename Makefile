install:
	npm install

bootstrap:
	node ./init.js

init:
	make install && make bootstrap

start:
	npx babel-node --extensions ".ts" -- src/engine/index.ts

publish:
	npm publish

lint:
	npx eslint .

test:
	npm test

.PHONY: test, ananasposter
