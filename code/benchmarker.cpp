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
#include <fcntl.h>

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
		} else {
			int flg = fcntl(fd, F_GETFL, 0);
			fcntl(fd, F_SETFL, flg | O_NONBLOCK);
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

	int SocketStateChanged(int fd, int timeout)
	{
		fd_set fdsRead;
		fd_set fdsWrite;
		struct timeval tv = {0};

		FD_ZERO(&fdsRead);
		FD_ZERO(&fdsWrite);
		FD_SET(fd, &fdsRead);
		FD_SET(fd, &fdsWrite);

		tv.tv_sec = 10;
		tv.tv_usec = 0;

		struct timeval *ptv = &tv;

		int retval = select(fd + 1, &fdsRead, &fdsWrite, NULL, ptv);
		// select error
		if(retval == -1) {
			printf("select error\n");
			return -1;
		}
		// socket timeout
		else if(retval == 0) {
			printf("select timeout\n");
			return 0;
		} else {
			int flg = 0;

			if(FD_ISSET(fd, &fdsRead)) {
				flg |= 1;
			}

			if(FD_ISSET(fd, &fdsWrite)) {
				flg |= 2;
			}

			return flg;
		}

		return -1;
	}

	int SocketStateChanged(int fd, int timeout, bool selectWrite)
	{
		fd_set fdsRead;
		fd_set fdsWrite;
		struct timeval tv = {0};

		FD_ZERO(&fdsRead);
		FD_ZERO(&fdsWrite);
		FD_SET(fd, &fdsRead);
		FD_SET(fd, &fdsWrite);

		tv.tv_sec = 10;
		tv.tv_usec = 0;

		struct timeval *ptv = &tv;

		int retval = select(fd + 1, &fdsRead, selectWrite ? &fdsWrite : NULL, NULL, ptv);
		// select error
		if(retval == -1) {
			printf("select error\n");
			return -1;
		}
		// socket timeout
		else if(retval == 0) {
			printf("select timeout\n");
			return 0;
		} else {
			int flg = 0;

			if(FD_ISSET(fd, &fdsRead)) {
				flg |= 1;
			}

			if(selectWrite) {
				if(FD_ISSET(fd, &fdsWrite)) {
					flg |= 2;
				}
			}

			return flg;
		}

		return -1;
	}

	bool ClearSocket(int fd)
	{
		static char buf[1024 * 64];
		for(int i = 0; i < 3; i++) {
			int len = read(fd, buf, sizeof(buf));
			if(len == -1) {
				//printf("server reset connection\n");
				//return false;
			} else if(len == 0) {
				break;
			} else {
				buf[len] = 0;
				printf("%s", buf);
			}
		}

		return true;
	}
}

static const int MAX_MSG_HEADER_BUFFER_SIZE = 1024 * 1024;
static char g_bufMsgHeader[MAX_MSG_HEADER_BUFFER_SIZE];

static const int MAX_MSG_BODY_BUFFER_SIZE = 1024 * 1024;
static char g_bufMsgBody[MAX_MSG_BODY_BUFFER_SIZE];

int buildMessageHeader(const char* topic, int maxPartition, int msgBodySize)
{
	static unsigned int opaque = 0;
	static unsigned int partition = 0;

	int len = sprintf(g_bufMsgHeader, "put %s %u %d %d %u\r\n"
	                  , topic
	                  , partition++ % maxPartition
	                  , msgBodySize
	                  , 0
	                  , opaque++);
	//printf("[%s][%d]\n", g_bufMsgHeader, len);

	return len;
}

void sendMessageAlways(int fd, const char* topic, int maxPartition, int msgBodySize, int dumyThreads)
{
	long long requestTimes = 0;
	long long responseTimes = 0;
SENDMESSAGEALWAYS_AGAIN:
	int msgHeaderLength = buildMessageHeader(topic, maxPartition, msgBodySize);
	int writeHeaderLength = 0;
	int writeBodyLength = 0;

	requestTimes++;


SENDMESSAGEALWAYS_SELECT:
	bool selectWrite = (requestTimes - responseTimes) < dumyThreads;
	int socketState = SimpleUtil::SocketStateChanged(fd, 10, selectWrite);
	if(socketState == -1) {
		return;
	} else if(socketState == 0) {
	} else {
		// read
		if((socketState & 1) == 1) {
			static char bufRead[1024 * 64];
			for(int i = 0; i < 3; i++) {
				int len = read(fd, bufRead, sizeof(bufRead));
				if(len == -1) {
				} else if(len == 0) {
					break;
				} else {
					for(int k = 0; k < len; k++) {
						if(bufRead[k] == 'r') responseTimes ++;
					}
				}
			}
		}

		// write
		if((socketState & 2) == 2) {
SENDMESSAGEALWAYS_WRITE:
			if(writeHeaderLength < msgHeaderLength) {
				int len = write(fd, g_bufMsgHeader + writeHeaderLength, msgHeaderLength - writeHeaderLength);
				if(len > 0) {
					writeHeaderLength += len;
					goto SENDMESSAGEALWAYS_WRITE;
				} else if(len == 0) {
					goto SENDMESSAGEALWAYS_SELECT;
				} else if(len == -1) {
					printf("write error\n");
					return;
				}
			} else if(writeBodyLength < msgBodySize) {
				int len = write(fd, g_bufMsgBody + writeBodyLength, msgBodySize - writeBodyLength);
				if(len > 0) {
					writeBodyLength += len;
					goto SENDMESSAGEALWAYS_WRITE;
				} else if(len == 0) {
					goto SENDMESSAGEALWAYS_SELECT;
				}  else if(len == -1) {
					printf("write error\n");
					return;
				}
			} else {
				goto SENDMESSAGEALWAYS_AGAIN;
			}

			printf("never do this 1\n");
		} else {
			goto SENDMESSAGEALWAYS_SELECT;
		}

		printf("never do this 2\n");
	}
}

int main(int argc, char** argv)
{
	if(argc != 6) {
		printf("%s serverUrl topic maxPartition msgBodySize dumyThreads\n", argv[0]);
		return -1;
	}

	std::string serverUrl = argv[1];
	std::string topic = argv[2];
	int maxPartition = atoi(argv[3]);
	int msgBodySize = atoi(argv[4]);
	int dumyThreads = atoi(argv[5]);

	memset(g_bufMsgBody, 'H', MAX_MSG_BODY_BUFFER_SIZE);

	while(true) {
		int fd = SimpleUtil::ConnectRemoteHost(serverUrl.c_str());
		if(-1 == fd) {
			printf("ConnectRemoteHost failed\n");
		}

		sendMessageAlways(fd, topic.c_str(), maxPartition, msgBodySize, dumyThreads);

		printf("sendMessageAlways over\n");

		SimpleUtil::CloseSocket(fd);
		sleep(3);
	}
	return 0;
}
