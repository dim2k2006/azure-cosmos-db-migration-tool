install:
	npm install

start:
	npx babel-node --extensions ".ts" -- src/engine/index.ts

publish:
	npm publish

lint:
	npx eslint .

test:
	npm test

.PHONY: test, ananasposter
