.PHONY: all

all: MaxElev.jar

%.jar: %.class %$%Mapper.class %$%Reducer.class
	jar cf $@ $<

%.class %$%Mapper.class %$%Reducer.class: %.java
	hadoop com.sun.tools.javac.Main $<
