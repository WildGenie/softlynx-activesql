/*
  .c -- Binary patcher

  Copyright 2003 Colin Percival

  For the terms under which this work may be distributed, please see
  the adjoining file "LICENSE".
  
  Changes 23 October 2004 by Timotheus Pokorra (timotheus@pokorra.de):
  	compiles now under Dev-C++ on Windows.
  	uses bzlib, available as a package for Dev-C++
  
*/

#include <wchar.h>
#include <sys/types.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <fcntl.h>
#include "bzlib.h"

typedef unsigned char u_char;

void  errx(int a, wchar_t * format, wchar_t * param)
{
	wchar_t buf[1024];
	swprintf_s<1024>(buf,format,param);
	System::Exception^ ex=gcnew System::Exception(gcnew System::String(buf));
	throw ex;
}
void errx(int a, wchar_t * message)
{
	errx(a,L"%s",message);
}

size_t loopread(BZFILE* d,int*bzerror,u_char *buf,size_t nbytes)
{
	size_t ptr,lenread;

	for(ptr=0;ptr<nbytes;ptr+=lenread) {
		lenread=BZ2_bzread(d, buf+ptr,nbytes-ptr);
		if (*bzerror != BZ_OK) errx(1, L"bzread");
		if(lenread==0) return ptr;
		if(lenread==-1) return -1;
	};
	return ptr;
}

FILE* bz2read(BZFILE** bz, int* bzerror, off_t offset,off_t len,wchar_t * fname)
{
	u_char * data;
	FILE* fd2;
	if((fd2=_wfopen(fname,L"rb"))<=0) 
		errx(1,L"problem read %s",fname);
	
	if((fseek(fd2,offset,SEEK_SET) != 0 || (ftell(fd2) !=offset)))
		errx(1,L"problem seek %s",fname);

    *bz = BZ2_bzReadOpen(bzerror, fd2, 0, 0, NULL, 0);
    if (*bzerror != BZ_OK) 
    {
        errx(1,L"bzreadopen");
    }    
	return fd2;
}

off_t offtin(u_char *buf)
{
	off_t y;

	y=buf[7]&0x7F;
	y=y*256;y+=buf[6];
	y=y*256;y+=buf[5];
	y=y*256;y+=buf[4];
	y=y*256;y+=buf[3];
	y=y*256;y+=buf[2];
	y=y*256;y+=buf[1];
	y=y*256;y+=buf[0];

	if(buf[7]&0x80) y=-y;

	return y;
}

int bspatch(wchar_t * argv[])
{
    FILE* ctrlpipe,*diffpipe,*extrapipe;
    BZFILE* ctrlbz, *diffbz, *extrabz;
    int ctrlerror, differror, extraerror;
	FILE* fd;
	size_t patchsize,oldsize,newsize;
	size_t bzctrllen,bzdatalen;
	u_char header[32],buf[8];
	int version=0;
	u_char *old, *pnew;
	off_t oldpos,newpos;
	off_t ctrl[3];
	off_t lenread;
	off_t i;

	if(((fd=_wfopen(argv[3],L"rb"))<=0) ||
		(fseek(fd,0,SEEK_END)!=0) ||
		((patchsize=ftell(fd)) == 0) ||
		(fseek(fd,0,SEEK_SET)!=0)) errx(1,L"Problem patchsize %s",argv[3]);
	if(patchsize<32) errx(1,L"Corrupt patch\n");

	/*
	  Ok, this is going to be messy.  There are two different patch
	formats which we need to support.

	  The old format (pre-4.0) is:
		0	8	"QSUFDIFF" or "BSDIFF30"
		8	8	X
		16	8	Y
		24	8	sizeof(newfile)
		32	X	bzip2(control block)
		32+X	Y	bzip2(data block)
	with control block a set of pairs (x,y) meaning "seek forward
	in oldfile by y bytes, and add the next x bytes to x bytes from
	the data block".

	  The new format (4.0) is:
		0	8	"BSDIFF40"
		8	8	X
		16	8	Y
		24	8	sizeof(newfile)
		32	X	bzip2(control block)
		32+X	Y	bzip2(diff block)
		32+X+Y	???	bzip2(extra block)
	with control block a set of triples (x,y,z) meaning "add x bytes
	from oldfile to x bytes from the diff block; copy y bytes from the
	extra block; seek forwards in oldfile by z bytes".
	*/

	if(fread(header,1,32,fd)!=32) errx(1,L"Error reading %s",argv[3]);
	if(memcmp(header,"QSUFDIFF",8)==0) version=1;
	if(memcmp(header,"BSDIFF30",8)==0) version=1;
	if(memcmp(header,"BSDIFF40",8)==0) version=2;

	if(!version) errx(1,L"Corrupt patch\n");

	bzctrllen=offtin(header+8);
	bzdatalen=offtin(header+16);
	newsize=offtin(header+24);
	if((bzctrllen<0) || (bzdatalen<0) || (newsize<0) ||
		((version==1) && (32+bzctrllen+bzdatalen!=patchsize)))
		errx(1,L"Corrupt patch\n");

	ctrlpipe=bz2read(&ctrlbz, &ctrlerror, 32,bzctrllen,argv[3]);
	diffpipe=bz2read(&diffbz, &differror, 32+bzctrllen,bzdatalen,argv[3]);
	if(version==2) {
		extrapipe=bz2read(&extrabz, &extraerror, 32+bzctrllen+bzdatalen,
			patchsize-(32+bzctrllen+bzdatalen),argv[3]);
	};

	if(fclose(fd)==-1) errx(1,L"Problem closing %s",argv[3]);
	if(((fd=_wfopen(argv[1],L"rb"))<=0) ||
		(fseek(fd,0,SEEK_END)!=0) ||
		((oldsize=ftell(fd))==-1) ||
		((old=(u_char*)malloc(oldsize+1))==NULL) ||
		(fseek(fd,0,SEEK_SET)!=0) ||
		(fread(old,1,oldsize,fd)!=oldsize) ||
		(fclose(fd)==-1)) errx(1,L"Can't handle %s",argv[1]);
	if((pnew=(u_char*)malloc(newsize+1))==NULL) errx(1,NULL);

	oldpos=0;newpos=0;
	while(newpos<newsize) {
		for(i=0;i<=version;i++) {
			if((lenread=loopread(ctrlbz,&ctrlerror,buf,8))<0) errx(1,NULL);
			if(lenread<8) errx(1,L"1: Corrupt patch\n");
			ctrl[i]=offtin(buf);
		};

		if(version==1) oldpos+=ctrl[1];

		if(newpos+ctrl[0]>newsize) errx(1,L"2: Corrupt patch\n");
		if((lenread=loopread(diffbz,&differror,pnew+newpos,ctrl[0]))<0)
			errx(1,NULL);
		if(lenread!=ctrl[0]) errx(1,L"3: Corrupt patch\n");
		for(i=0;i<ctrl[0];i++)
			if((oldpos+i>=0) && (oldpos+i<oldsize))
				pnew[newpos+i]+=old[oldpos+i];
		newpos+=ctrl[0];
		oldpos+=ctrl[0];

		if(version==2) {
			if(newpos+ctrl[1]>newsize) errx(1,L"4: Corrupt patch\n");
			if((lenread=loopread(extrabz,&extraerror,pnew+newpos,ctrl[1]))<0)
				errx(1,NULL);
			if(lenread!=ctrl[1]) errx(1,L"5: Corrupt patch\n");

			newpos+=ctrl[1];
			oldpos+=ctrl[2];
		};
	};

	if(loopread(ctrlbz,&ctrlerror,buf,1)!=0) errx(1,L"6: Corrupt patch\n");
	if(loopread(diffbz,&differror,buf,1)!=0) errx(1,L"7:Corrupt patch\n");
	if(version==2)
		if(loopread(extrabz,&extraerror,buf,1)!=0) errx(1,L"8: Corrupt patch\n");


    BZ2_bzReadClose(&ctrlerror, ctrlbz);
    BZ2_bzReadClose(&differror, diffbz);
    if (version == 2) 
    {
        BZ2_bzReadClose(&extraerror, extrabz);
    }    
	if(fclose(ctrlpipe) || fclose(diffpipe) || 
		((version==2) && fclose(extrapipe)))
		errx(1,NULL);

	if(((fd=_wfopen(argv[2],L"wb"))<=0) ||
		(fwrite(pnew,1,newsize,fd)!=newsize) || (fclose(fd)==-1))
		errx(1,L"Can't handle %s",argv[2]);

	free(pnew);
	free(old);

	return 0;
}
