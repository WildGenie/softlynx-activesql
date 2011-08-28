/*-
 * Copyright 2003-2005 Colin Percival
 * All rights reserved
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted providing that the following conditions 
 * are met:
 * 1. Redistributions of source code must retain the above copyright
 *    notice, this list of conditions and the following disclaimer.
 * 2. Redistributions in binary form must reproduce the above copyright
 *    notice, this list of conditions and the following disclaimer in the
 *    documentation and/or other materials provided with the distribution.
 *
 * THIS SOFTWARE IS PROVIDED BY THE AUTHOR ``AS IS'' AND ANY EXPRESS OR
 * IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
 * WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED.  IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR ANY
 * DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
 * DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS
 * OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION)
 * HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT,
 * STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING
 * IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
 */

#include <tchar.h>
#include <sys/types.h>
#include "bzlib.h"
//#include <err.h>
#include <fcntl.h>
#include <io.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
//#include <string>
#include "vcclr.h"

#include <iostream>
#include <string>


using namespace std;
using namespace System;
using namespace System::Runtime::InteropServices;

wchar_t * wcharstr(System::String^ s)
{
	wchar_t * res=new wchar_t[s->Length+1];
    pin_ptr<const wchar_t> wch = PtrToStringChars(s);
    wcscpy_s(res,s->Length+1, wch);
	return res;
}

char * charstr(System::String^ s)
{
   
   // Convert to a char*
	pin_ptr<const wchar_t> wch = PtrToStringChars(s);
    size_t origsize = wcslen(wch) + 1;
    const size_t newsize = origsize*4;
    size_t convertedChars = 0;
    char * nstring=new char[newsize];

    wcstombs_s(&convertedChars, nstring, origsize, wch, _TRUNCATE);
	return nstring;
}
    


extern int bsdiff(wchar_t *argv[]);
extern int bspatch(wchar_t *argv[]);

namespace BSDiffTools
{

public ref class  Patch 
{
public:
static  void Create(System::String^ oldfile,System::String^ newfile, System::String^ patchfile) 
{
	wchar_t * _oldfile=wcharstr(oldfile);
	wchar_t * _newfile=wcharstr(newfile);
	wchar_t * _patchfile=wcharstr(patchfile);
	wchar_t * params[]={L"",_oldfile,_newfile,_patchfile};
	bsdiff(params);
	//wcout << _oldfile;
	//CreatePatch(_oldfile,_newfile,_patchfile);
	delete _oldfile;
	delete _newfile;
	delete _patchfile;
};

static  void Apply(System::String^ oldfile,System::String^ newfile, System::String^ patchfile) 
{
	wchar_t * _oldfile=wcharstr(oldfile);
	wchar_t * _newfile=wcharstr(newfile);
	wchar_t * _patchfile=wcharstr(patchfile);
	wchar_t * params[]={L"",_oldfile,_newfile,_patchfile};
	bspatch(params);
	delete _oldfile;
	delete _newfile;
	delete _patchfile;
};

};

}