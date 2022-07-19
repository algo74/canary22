#include <stdio.h>
#include <stdlib.h>
#include <time.h>

#include <fcntl.h>
#include <unistd.h>
#include <getopt.h>


#include <errno.h>
#include <lustre/lustreapi.h>

#ifndef O_BINARY
#define O_BINARY 0
#endif


/* Open a file, set a specific stripe count, size and starting OST
 *    Adjust the parameters to suit */
int open_stripe_file(const char *tfile, const int mode, const int stripe_offset)
{
  int stripe_size = 65536;    /* System default is 4M */
//  int stripe_offset = -1;     /* Start at default */
  int stripe_count = 1;  /*Single stripe for this demo*/
  int stripe_pattern = 0;     /* only RAID 0 at this time */
  int rc, fd;

  rc = llapi_file_open(tfile, O_CREAT | O_WRONLY | O_BINARY, mode,
                         stripe_size,stripe_offset,stripe_count,stripe_pattern);
  /* result code is inverted, we may return -EINVAL or an ioctl error.
   * We borrow an error message from sanity.c
   */
  if (rc) {
    fprintf(stderr,"llapi_file_open failed: %d (%s) \n", rc, strerror(-rc));
    return -1;
  }
  return fd;
}



void do_write(size_t b_size, size_t fileSize, char *fileContent, int out)
{
  for (char *p = fileContent; p < fileContent + fileSize; p += b_size)
  {
    write(out, p, b_size);
  }
}


double testPosixIO(const char* outFile, const size_t ost)
{
  size_t b_size = 64 * 1024;
  size_t s_count = 64;
  clock_t start_t, end_t;

  size_t fileSize = s_count * b_size;
  // allocate buffer for the file content
  char *fileContent = malloc(fileSize);
  if (fileContent == 0) {
    fprintf(stderr, "Can't allocate buffer of size %zu\n", fileSize);
    return -1;
  }
  // TODO: fill up the file content
  // open the file
  int out = open_stripe_file(outFile, 0666, ost);
  if (out < 0)
  {
    fprintf(stderr, "Can't open output file %s on OST %zu\n", outFile, ost);
    return -1;
  }

  // Write content to the file
  start_t = clock();
  do_write(b_size, fileSize, fileContent, out);
  end_t = clock();
  // close the file
  close(out);
  return (double)(end_t - start_t) / CLOCKS_PER_SEC;
}


void usage(FILE *f, char *exe)
{
  fprintf(f, "Usage: %s { {-h|--help} | {-f|--filename} <filename> {-o|--ost} <OST #> }\n", exe);
}


int main(int argc, char* argv[])
{
  char filename[256] = { 0 };
  size_t ost = SIZE_MAX;
  struct option longopts[] = {
      { "help", no_argument, NULL, 'h' },
      { "filename", required_argument, NULL, 'f' },
      { "ost", required_argument, NULL, 'o' },
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
    int opt = getopt_long (argc, argv, "ho:f:", longopts, 0);

    if (opt == -1) {
      /* a return value of -1 indicates that there are no more options */
      break;
    }

    switch (opt) {
      case 'h':
        /* the help_flag and value are specified in the longopts table,
         * which means that when the --help option is specified (in its long
         * form), the help_flag variable will be automatically set.
         * however, the parser for short-form options does not support the
         * automatic setting of flags, so we still need this code to set the
         * help_flag manually when the -h option is specified.
         */
        usage (stderr, argv[0]);
        return 0;
      case 'f':
        /* optarg is a global variable in getopt.h. it contains the argument
         * for this option. it is null if there was no argument.
         */
        if (strlen(optarg) >= sizeof(filename)) {
          fprintf("Error: filename longer than 255: %s\n", optarg);
          usage (stderr, argv[0]);
          return 1;
        }
        strncpy (filename, optarg, sizeof(filename) - 1);
        filename[sizeof (filename) - 1] = '\0';
        break;
      case 'o':
        ; // need a statement after the label and "declaration is not a statement"
        char *pEnd = 0;
        ost = strtoul(optarg, &pEnd, 10);
        if (pEnd != optarg + strlen(optarg)) {
          fprintf("Error: bad value for OST (non-negative integer is expected): %uz\n", optarg);
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

  double duration = testPosixIO(filename, ost);

  printf("%f", duration);
  return 0;
}