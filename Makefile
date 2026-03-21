.PHONY: all console pm clean

all: pm

console:
	cd pm/console/web && npm install && npm run build

pm: console
	go build -o bin/pm ./cmd/pm

clean:
	rm -rf bin/pm pm/console/web/node_modules pm/console/web/dist
	# Restore placeholder so go:embed stays valid
	mkdir -p pm/console/web/dist
	echo '<!doctype html><html><body><p>Run make console to build.</p></body></html>' > pm/console/web/dist/index.html
