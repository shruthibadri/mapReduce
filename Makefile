.PHONY: all

all: MaxElev.jar

MaxElev.jar: MaxElev.class
	jar cf $@ $<

MaxElev.class: MaxElev.java
	hadoop com.sun.tools.javac.Main $<
