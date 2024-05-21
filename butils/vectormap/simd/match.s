//go:build amd64

#include "textflag.h"

TEXT ·MatchMetadata(SB), NOSPLIT, $0-18
	MOVQ     metadata+0(FP), AX
	MOVBLSX  hash+8(FP), CX
	MOVD     CX, X0
	PXOR     X1, X1
	PSHUFB   X1, X0
	MOVOU    (AX), X1
	PCMPEQB  X1, X0
	PMOVMSKB X0, AX
	MOVW     AX, ret+16(FP)
	RET
