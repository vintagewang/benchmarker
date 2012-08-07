#
# $Id: makefile 770 2011-12-12 07:20:10Z shijia.wxr $
#

TOPDIR = ..

all: benchmarker

clean:
	rm -f *.o
	rm -f getclient
	rm -f setclient
	rm -f getclient2
	rm -f setclient2
	rm -f clientlistener

getclient: getclient.cpp
	g++ -o $@ $< $(TOPDIR)/lib/libdiamond.a -I$(TOPDIR)/include -L$(TOPDIR)/lib -lz -lcrypt -lldap -lrt -lidn

setclient: setclient.cpp
	g++ -o $@ $< $(TOPDIR)/lib/libdiamond.a -I$(TOPDIR)/include -L$(TOPDIR)/lib -lz -lcrypt -lldap -lrt -lidn

getclient2: getclient2.cpp
	g++ -o $@ $< $(TOPDIR)/lib/libdiamond.a -I$(TOPDIR)/include -L$(TOPDIR)/lib -lz -lcrypt -lldap -lrt -lidn

setclient2: setclient2.cpp
	g++ -o $@ $< $(TOPDIR)/lib/libdiamond.a -I$(TOPDIR)/include -L$(TOPDIR)/lib -lz -lcrypt -lldap -lrt -lidn

clientlistener: clientlistener.cpp
	g++ -o $@ $< $(TOPDIR)/lib/libdiamond.a -I$(TOPDIR)/include -L$(TOPDIR)/lib -lz -lcrypt -lldap -lrt -lidn
