include $(GOROOT)/src/Make.inc

.SUFFIXES: .go .$O

OBJS=mc_constants.$O \
		 byte_manipulation.$O \
         mc.$O \
         controller.$O \
		 gohammer.$O

gohammer: $(OBJS)
	$(LD) -o gohammer gohammer.$O

clean:
	rm -f $(OBJS) gohammer

.go.$O:
	$(GC) $<
