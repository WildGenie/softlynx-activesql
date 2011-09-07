#include <stdlib.h>
#include <signal.h>
#include <winerror.h>
#include "xdelta3.h"


int code (
  int encode,
  FILE*  InFile,
  FILE*  SrcFile ,
  FILE* OutFile,
  int BufSize )
{
  int r, ret;
  xd3_stream stream;
  xd3_config config;
  xd3_source source;
  uint8_t * Input_Buf;
  int Input_Buf_Read;

  if (BufSize < XD3_ALLOCSIZE)
    BufSize = XD3_ALLOCSIZE;

  memset (&stream, 0, sizeof (stream));
  memset (&source, 0, sizeof (source));

  xd3_init_config(&config, XD3_ADLER32);
  config.winsize = BufSize;
  xd3_config_stream(&stream, &config);

  if (SrcFile)
  {

    source.blksize = BufSize;
    source.curblk = (uint8_t*) malloc(source.blksize);

    /* Load 1st block of stream. */
    r = fseek(SrcFile, 0, SEEK_SET);
    if (r)
      return r;
    source.onblk = fread((void*)source.curblk, 1, source.blksize, SrcFile);
    source.curblkno = 0;
    /* Set the stream. */
    xd3_set_source(&stream, &source);
  }

  Input_Buf = (uint8_t*) malloc(BufSize);

  fseek(InFile, 0, SEEK_SET);
  do
  {
    Input_Buf_Read = fread(Input_Buf, 1, BufSize, InFile);
    if (Input_Buf_Read < BufSize)
    {
      xd3_set_flags(&stream, XD3_FLUSH | stream.flags);
    }
    xd3_avail_input(&stream, Input_Buf, Input_Buf_Read);

process:
    if (encode)
      ret = xd3_encode_input(&stream);
    else
      ret = xd3_decode_input(&stream);

    switch (ret)
    {
    case XD3_INPUT:
      {
        fprintf (stderr,"XD3_INPUT\n");
        continue;
      }

    case XD3_OUTPUT:
      {
        fprintf (stderr,"XD3_OUTPUT\n");
        r = fwrite(stream.next_out, 1, stream.avail_out, OutFile);
        if (r != (int)stream.avail_out)
          return r;
	xd3_consume_output(&stream);
        goto process;
      }

    case XD3_GETSRCBLK:
      {
        fprintf (stderr,"XD3_GETSRCBLK %qd\n", source.getblkno);
        if (SrcFile)
        {
          r = fseek(SrcFile, source.blksize * source.getblkno, SEEK_SET);
          if (r)
            return r;
          source.onblk = fread((void*)source.curblk, 1,
			       source.blksize, SrcFile);
          source.curblkno = source.getblkno;
        }
        goto process;
      }

    case XD3_GOTHEADER:
      {
        fprintf (stderr,"XD3_GOTHEADER\n");
        goto process;
      }

    case XD3_WINSTART:
      {
        fprintf (stderr,"XD3_WINSTART\n");
        goto process;
      }

    case XD3_WINFINISH:
      {
        fprintf (stderr,"XD3_WINFINISH\n");
        goto process;
      }

    default:
      {
        fprintf (stderr,"!!! INVALID %s %d !!!\n",
		stream.msg, ret);
        return ret;
      }

    }

  }
  while (Input_Buf_Read == BufSize);

  free(Input_Buf);

  free((void*)source.curblk);
  xd3_close_stream(&stream);
  xd3_free_stream(&stream);

  return 0;

};

int wmain(int argc, wchar_t** argv)
{
  int r;
  FILE*  InFile;
  FILE*  SrcFile;
  FILE* OutFile;

  if (argc != 4) {
	MessageBox(0,L"<source> <patch> <destination>", L"Usage:",MB_OK);
    return 1;
  };

  SrcFile = _wfopen(argv[1], L"rb");
  InFile = _wfopen(argv[2], L"rb");
  OutFile = _wfopen(argv[3], L"wb");

  r = code (0, InFile, SrcFile, OutFile, 0x1000);

  fclose(OutFile);
  fclose(SrcFile);
  fclose(InFile);

  if (r) {
    fprintf (stderr, "Decode error: %d\n", r);
    return r;
  }

  return 0;

/*  
	  reset_defaults();

	  main_file_init (& ifile);
	  main_file_init (& ofile);
	  main_file_init (& sfile);
	  
	  int ret = 0; // main_input (CMD_DECODE, & ifile, & ofile, & sfile);

	  main_file_cleanup (& ifile);
	  main_file_cleanup (& ofile);
	  main_file_cleanup (& sfile);

	  main_cleanup();
	  return ret;
	  */
	//return main(argc,argv);
}
