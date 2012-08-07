#include <assert.h>
#include <limits.h>
#include <string.h>
#include <string>

#include <sys/types.h>
#include <sys/socket.h>
#include <netdb.h>
#include <arpa/inet.h>
#include <sys/ioctl.h>
#include <linux/if.h>
#include <netinet/tcp.h> // for TCP_NODELAY
#include <sys/select.h>
#include <sys/time.h>
#include <unistd.h>

namespace SimpleUtil
{
	void CloseSocket(int fd)
	{
		if(fd >= 0) {
			shutdown(fd, SHUT_RDWR);
			close(fd);
		}
	}

	bool HostName2Value(const char* fromhost, std::string& tohost)
	{
		assert(NULL != fromhost);

		bool result = (INADDR_NONE == inet_addr(fromhost));

		// 如果传入的是域名
		if(result) {
			struct hostent* host_entry = gethostbyname(fromhost);
			result = (host_entry != NULL);
			if(result) {
				char buf[64] = {0};
				sprintf(buf, "%d.%d.%d.%d",
				        (host_entry->h_addr_list[0][0] & 0x00ff),
				        (host_entry->h_addr_list[0][1] & 0x00ff),
				        (host_entry->h_addr_list[0][2] & 0x00ff),
				        (host_entry->h_addr_list[0][3] & 0x00ff));

				tohost = buf;
			}
		}
		// 如果传入的是IP地址
		else {
			result = true;
			tohost = fromhost;
		}

		return result;
	}

	bool SplitAddr(const char* addr, std::string& host, int& port)
	{
		assert(addr != NULL);

		std::string strAddr = addr;
		bool bResult = false;

		if(strAddr == "0") {
			host = "0.0.0.0";
			port = 0;
			bResult = true;
		} else {
			char buf[2][128];

			memset(buf, 0, sizeof(buf));

			// 先分割成HOST与PORT
			int ret = sscanf(addr, "%[^ :]%*[ :]%[^$]", buf[0], buf[1]);
			if(ret == 2) {
				host = buf[0];
				port = atoi(buf[1]);
				bResult = true;
			}
		}

		return bResult;
	}

	int ConnectRemoteHost(const char* host, int port)
	{
		assert(host != NULL);
		assert(port > 0);

		int fd = socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);
		bool bRetResult = (fd != -1);

		int optvalue = 0;
		bRetResult = bRetResult && (-1 != setsockopt(fd, SOL_SOCKET, SO_KEEPALIVE, &optvalue, (int)sizeof(optvalue)));

		optvalue = 1;
		bRetResult = bRetResult && (-1 != setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &optvalue, (int)sizeof(optvalue)));

		optvalue = 1;
		bRetResult = bRetResult && (-1 != setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &optvalue, (int)sizeof(optvalue)));

		optvalue = 1024 * 64;
		bRetResult = bRetResult && (-1 != setsockopt(fd, SOL_SOCKET, SO_SNDBUF, &optvalue, (int)sizeof(optvalue)));

		optvalue = 1024 * 64;
		bRetResult = bRetResult && (-1 != setsockopt(fd, SOL_SOCKET, SO_RCVBUF, &optvalue, (int)sizeof(optvalue)));

		// 尝试域名解析
		std::string hostNew;
		bRetResult = bRetResult && HostName2Value(host, hostNew);

		struct sockaddr_in addr = {0};
		addr.sin_family = AF_INET;
		addr.sin_addr.s_addr = inet_addr(hostNew.c_str());
		addr.sin_port = htons(port);
		bRetResult = bRetResult && (-1 != connect(fd, (struct sockaddr*)&addr, (int)sizeof(addr)));

		if(!bRetResult && -1 != fd) {
			CloseSocket(fd);
			fd = -1;
		}

		return fd;
	}

	int ConnectRemoteHost(const char* addr)
	{
		assert(NULL != addr);
		std::string host;
		int port = 0;

		if(SplitAddr(addr, host, port)) {
			host = (host == "0.0.0.0") ? "127.0.0.1" : host;
			return ConnectRemoteHost(host.c_str(), port);
		}

		return -1;
	}
}

int main(int argc, char** argv)
{
	int fd = SimpleUtil::ConnectRemoteHost(argv[1]);
	printf("ConnectRemoteHost %d\n", fd);
	return 0;
}
