all: 
	bash build.sh

linux: clean
	bash build.sh linux

clean:
	rm -rf bin
	rm -rf *.pprof
	rm -rf *.output
	rm -rf logs
	rm -rf diagnostic/
	rm -rf data/
	rm -rf *.pid
	
