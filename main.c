#include <stdio.h>
#include <stdlib.h>
#include <time.h>
#include <string.h>
#include <fcntl.h>
#include <unistd.h>
#include <getopt.h>


#include <errno.h>
#include <lustre/lustreapi.h>

#ifndef O_BINARY
#define O_BINARY 0
#endif

#include "simple_server/client.h"

typedef struct timespec my_time;

my_time get_precise_time()
{
  my_time ts;
  clock_gettime(CLOCK_BOOTTIME, &ts);
  return ts;
}


double time_diff(my_time start, my_time end)
{
  int sec = end.tv_sec - start.tv_sec;
  int nsec = end.tv_nsec - start.tv_nsec;
  return (double) sec + 1e-9 * (double) nsec;
}


long get_ts()
{
  return (long) time(NULL);
}


int send_to_server(int sockfd, size_t start_ts, size_t end_ts, size_t ost, double duration, size_t fileSize)
{
  const int maxlen = 1024;
  char buffer[maxlen] = { 0 };
  // TODO: proper encoding error handling
  cJSON *req =  cJSON_CreateObject();
  cJSON_AddStringToObject(req, "type", "process_canary_probe");
  snprintf(buffer, maxlen, "%zu", start_ts);
  cJSON_AddStringToObject(req, "start_time", buffer);
  snprintf(buffer, maxlen, "%zu", end_ts);
  cJSON_AddStringToObject(req, "end_time", buffer);
  snprintf(buffer, maxlen, "%zu", ost);
  cJSON_AddStringToObject(req, "OST", buffer);
  snprintf(buffer, maxlen, "%f", duration);
  cJSON_AddStringToObject(req, "duration", buffer);
  snprintf(buffer, maxlen, "%zu", fileSize);
  cJSON_AddStringToObject(req, "file_size", buffer);
  cJSON *resp = send_receive(sockfd, req);
  cJSON_Delete(req);
  // TODO: handle communication error messages here when simple_server is refactored
  if (resp == NULL) {
    rc = 33;
  } else {
    cJSON *payload = cJSON_GetObjectItem(resp, "status");
    if (strcmp(payload->valuestring, "ACK") == 0) {
      rc = 0;
    } else {
      rc = 44;
    }
    cJSON_Delete(resp);
  }
}


/* Open a file, set a specific stripe count, size and starting OST
 *    Adjust the parameters to suit */
int open_stripe_file(const char *tfile, const int mode, const int stripe_offset)
{
  const int stripe_size = 65536;    /* System default is 4M */
//  int stripe_offset = -1;     /* Start at default */
  const int stripe_count = 1;  /*Single stripe for this demo*/
  const int stripe_pattern = 0;     /* only RAID 0 at this time */

//  printf("O_BINARY: %d\n", O_BINARY);

  int fd = llapi_file_open(tfile, O_CREAT | O_WRONLY | O_BINARY, mode,
                         stripe_size,stripe_offset,stripe_count,stripe_pattern);
  /* result code is inverted, we may return -EINVAL or an ioctl error.
   * We borrow an error message from sanity.c
   */
  if (fd < 0) {
    fprintf(stderr,"llapi_file_open failed: Error %d (%s) \n", -fd, strerror(-fd));
  }
  return fd;
}


int do_write(size_t b_size, size_t fileSize, char *fileContent, int out)
{
  for (char *p = fileContent; p < fileContent + fileSize;)
  {
    for (size_t to_write = b_size; to_write > 0;) {
      size_t written = write(out, p, to_write);
      if (written == -1) {
        return -1;
      }
      p += written;
      to_write -= written;
    }
    fsync(out);
  }
  return 0;
}


void do_write1(size_t b_size, size_t fileSize, char *fileContent, int out)
{
  // TODO: write() (and similar system calls) will transfer at
  //  most 0x7ffff000 (2,147,479,552) bytes, returning the number of
  //  bytes actually transferred.
  write(out, fileContent, fileSize);
  fsync(out);
}


void usage(FILE *f, char *exe)
{
  fprintf(f, "Usage: %s { {-h|--help} | {-f|--filename} <filename> {-o|--ost} <OST #>\n"
             "             [{-b|--block_size <KiB>] [{-s|--segments <count>] [{-t|--tries <count>]}\n", exe);
}


int main(int argc, char* argv[])
{
  /////////////////////////////////////////////////////////////
  // parse options
  char filename[1024] = { 0 };
  char server_string[1024] = {0 };
  char *host, *port;
  size_t ost = SIZE_MAX;
  size_t b_size = 64 * 1024;
  size_t s_count = 16;
  size_t n_tries = 5;

  struct option longopts[] = {
      { "help", no_argument, NULL, 'h' },
      { "filename", required_argument, NULL, 'f' },
      { "ost", required_argument, NULL, 'o' },
      { "segments", required_argument, NULL, 's' },
      { "block_size", required_argument, NULL, 'b' },
      { "tries", required_argument, NULL, 't' },
      { "address", required_argument, NULL, 'a' },
      { 0 }
  };

  /* infinite loop, to be broken when we are done parsing options */
  while (1) {
    /* getopt_long supports GNU-style full-word "long" options in addition
     * to the single-character "short" options which are supported by
     * getopt.
     * the third argument is a collection of supported short-form options.
     * these do not necessarily have to correlate to the long-form options.
     * one colon after an option indicates that it has an argument, two
     * indicates that the argument is optional. order is unimportant.
     */
    int opt = getopt_long (argc, argv, "ho:f:s:b:t:", longopts, 0);

    if (opt == -1) {
      /* a return value of -1 indicates that there are no more options */
      break;
    }

    switch (opt) {
      char *pEnd;
      case 'h':
        usage (stderr, argv[0]);
        return 0;
      case 'f':
        /* optarg is a global variable in getopt.h. it contains the argument
         * for this option. it is null if there was no argument.
         */
        if (strlen(optarg) >= sizeof(filename)) {
          fprintf(stderr, "Error: filename longer than %zu: %s\n", sizeof(filename)-1, optarg);
          usage (stderr, argv[0]);
          return 1;
        }
        strncpy (filename, optarg, sizeof(filename) - 1);
        filename[sizeof (filename) - 1] = '\0';
        break;
      case 'a':
        if (strlen(optarg) >= sizeof(server_string)) {
          fprintf(stderr, "Error: address longer than %zu: %s\n", sizeof(server_string) - 1, optarg);
          usage (stderr, argv[0]);
          return 1;
        }
        strncpy (server_string, optarg, sizeof(server_string) - 1);
        server_string[sizeof(server_string) - 1] = '\0';
        char * colon = strchr(server_string, ':');
        if (colon == 0 || colon == server_string) {
          printf("malformed sever string: \"%s\"\n", server_string);
          return 1;
        }
        *colon = '\0';
        host = server_string;
        port = colon + 1;
        break;
      case 'o':
        ost = strtoul(optarg, &pEnd, 10);
        if (pEnd != optarg + strlen(optarg)) {
          fprintf(stderr, "Error: bad value for OST (non-negative integer is expected): %s\n", optarg);
          usage (stderr, argv[0]);
          return 1;
        }
        break;
      case 's':
        s_count = strtoul(optarg, &pEnd, 10);
        if (s_count == 0 || pEnd != optarg + strlen(optarg)) {
          fprintf(stderr, "Error: bad value for number of segments (positive integer is expected): %s\n", optarg);
          usage (stderr, argv[0]);
          return 1;
        }
        break;
      case 'b':
        b_size = 1024 * strtoul(optarg, &pEnd, 10);
        if (b_size == 0 || pEnd != optarg + strlen(optarg)) {
          fprintf(stderr, "Error: bad value for block size (positive integer is expected): %s\n", optarg);
          usage (stderr, argv[0]);
          return 1;
        }
        break;
      case 't':
        n_tries = strtoul(optarg, &pEnd, 10);
        if (n_tries == 0 || pEnd != optarg + strlen(optarg)) {
          fprintf(stderr, "Error: bad value for number of tries (positive integer is expected): %s\n", optarg);
          usage (stderr, argv[0]);
          return 1;
        }
        break;
      case '?':
        /* a return value of '?' indicates that an option was malformed.
         * this could mean that an unrecognized option was given, or that an
         * option which requires an argument did not include an argument.
         */
        usage (stderr, argv[0]);
        return 1;
      default:
        break;
    }
  }
  int good = 1;
  if (filename[0] == 0) {
    good = 0;
    fprintf(stderr, "Filename option is required\n");
  }
  if (ost == SIZE_MAX) {
    good = 0;
    fprintf(stderr, "OST option is required\n");
  }

  if (good == 0) {
    usage(stderr, argv[0]);
    return 1;
  }


  /////////////////////////////////////////////////////////////
  // Doing the work
  my_time start_t, end_t;
  size_t fileSize = s_count * b_size;
  // allocate buffer for the file content
  char *fileContent = malloc(fileSize);
  if (fileContent == 0) {
    fprintf(stderr, "Can't allocate buffer of size %zu\n", fileSize);
    return 2;
  }
  // fill up the file content
  int x1 = rand();
  int x2 = rand();
  for (int *p = (int*)fileContent; p < (int*)(fileContent+fileSize); p+= 2) {
    *p = x1;
    *(p+1) = x2;
    x1++;
    x2 += 7905479;
  }

  // delete file in case it exists
  unlink(filename);

  // we repeat several times in case of an error during the write
  for (size_t i = 0; i < n_tries; i++) {
    // open the file
    int out = open_stripe_file(filename, 0666, ost);
    if (out < 0) {
      fprintf(stderr, "Can't open output file %s on OST %zu\n", filename, ost);
      return 3;
    }

    // Write content to the file
    start_t = get_precise_time();
    long start_ts = get_ts();
    int result = do_write(b_size, fileSize, fileContent, out);
    end_t = get_precise_time();
    long end_ts = get_ts();
    // close the file
    close(out);
    unlink(filename);

    if (result == 0) {
      // good experiment - write the result
      double duration = time_diff(start_t, end_t);
      if (host) {
        int sockfd = connect_to_simple_server(host, port);
        if (sockfd == -1) {
          fprintf(stderr, "Error connecting to server %s (port %s). Message not sent.\n", host, port);
        } else {
          send_to_server(sockfd, start_ts, end_ts, ost, duration, fileSize); // ignoring return code for now
        }
      }
      printf("%ld, %ld, %zu, %f, %zu\n", start_ts, end_ts, ost, duration, fileSize);
      free(fileContent);
      return 0;
    }
    fprintf(stderr, "Error writing to the file. Trying again...\n");
  }
  free(fileContent);
  fprintf(stderr, "Tried %zu times but could not write the file without errors. Aborting.\n", n_tries);
  return 4;
}
