.PHONY: all clean

all: MaxElev.jar

clean:
	rm *.class *.jar

define make_rule

$(1).jar: $(1).class
	jar cf $$@ $(1)*.class

$(1).class: $(1).java
	hadoop com.sun.tools.javac.Main $$<

endef

$(foreach i, MaxElev, $(eval $(call make_rule,$(i))))
