#ifndef _SIMPLE_SOCKET_H_
#define _SIMPLE_SOCKET_H_
#include <string>

namespace SimpleSocket
{
	int ConnectRemoteHost(const char* host, int port);
	int ConnectRemoteHost(const char* addr);
	void CloseSocket(int fd);
};

#endif // end of _SIMPLE_SOCKET_H_
