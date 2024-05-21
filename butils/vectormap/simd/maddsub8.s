//go:build !noasm && amd64
TEXT ·MAdd128epi8(SB), $0-32
	MOVQ a+0(FP), DI
	MOVQ b+8(FP), SI
	MOVQ out+16(FP), DX
	BYTE $0x55               // pushq	%rbp
	WORD $0x8948; BYTE $0xe5 // movq	%rsp, %rbp
	LONG $0xf8e48348         // andq	$-8, %rsp
	LONG $0x066ffac5         // vmovdqu	(%rsi), %xmm0
	LONG $0x07fcf9c5         // vpaddb	(%rdi), %xmm0, %xmm0
	LONG $0x027ffac5         // vmovdqu	%xmm0, (%rdx)
	WORD $0x8948; BYTE $0xec // movq	%rbp, %rsp
	BYTE $0x5d               // popq	%rbp
	BYTE $0xc3               // retq

TEXT ·MSub128epi8(SB), $0-32
	MOVQ a+0(FP), DI
	MOVQ b+8(FP), SI
	MOVQ out+16(FP), DX
	BYTE $0x55               // pushq	%rbp
	WORD $0x8948; BYTE $0xe5 // movq	%rsp, %rbp
	LONG $0xf8e48348         // andq	$-8, %rsp
	LONG $0x076ffac5         // vmovdqu	(%rdi), %xmm0
	LONG $0x06f8f9c5         // vpsubb	(%rsi), %xmm0, %xmm0
	LONG $0x027ffac5         // vmovdqu	%xmm0, (%rdx)
	WORD $0x8948; BYTE $0xec // movq	%rbp, %rsp
	BYTE $0x5d               // popq	%rbp
	BYTE $0xc3               // retq

TEXT ·MAdds128epi8(SB), $0-32
	MOVQ a+0(FP), DI
	MOVQ b+8(FP), SI
	MOVQ out+16(FP), DX
	BYTE $0x55               // pushq	%rbp
	WORD $0x8948; BYTE $0xe5 // movq	%rsp, %rbp
	LONG $0xf8e48348         // andq	$-8, %rsp
	LONG $0x076ffac5         // vmovdqu	(%rdi), %xmm0
	LONG $0x06ecf9c5         // vpaddsb	(%rsi), %xmm0, %xmm0
	LONG $0x027ffac5         // vmovdqu	%xmm0, (%rdx)
	WORD $0x8948; BYTE $0xec // movq	%rbp, %rsp
	BYTE $0x5d               // popq	%rbp
	BYTE $0xc3               // retq

TEXT ·MSubs128epi8(SB), $0-32
	MOVQ a+0(FP), DI
	MOVQ b+8(FP), SI
	MOVQ out+16(FP), DX
	BYTE $0x55               // pushq	%rbp
	WORD $0x8948; BYTE $0xe5 // movq	%rsp, %rbp
	LONG $0xf8e48348         // andq	$-8, %rsp
	LONG $0x076ffac5         // vmovdqu	(%rdi), %xmm0
	LONG $0x06e8f9c5         // vpsubsb	(%rsi), %xmm0, %xmm0
	LONG $0x027ffac5         // vmovdqu	%xmm0, (%rdx)
	WORD $0x8948; BYTE $0xec // movq	%rbp, %rsp
	BYTE $0x5d               // popq	%rbp
	BYTE $0xc3               // retq

TEXT ·MAdds128epu8(SB), $0-32
	MOVQ a+0(FP), DI
	MOVQ b+8(FP), SI
	MOVQ out+16(FP), DX
	BYTE $0x55               // pushq	%rbp
	WORD $0x8948; BYTE $0xe5 // movq	%rsp, %rbp
	LONG $0xf8e48348         // andq	$-8, %rsp
	LONG $0x076ffac5         // vmovdqu	(%rdi), %xmm0
	LONG $0x06dcf9c5         // vpaddusb	(%rsi), %xmm0, %xmm0
	LONG $0x027ffac5         // vmovdqu	%xmm0, (%rdx)
	WORD $0x8948; BYTE $0xec // movq	%rbp, %rsp
	BYTE $0x5d               // popq	%rbp
	BYTE $0xc3               // retq

TEXT ·MSubs128epu8(SB), $0-32
	MOVQ a+0(FP), DI
	MOVQ b+8(FP), SI
	MOVQ out+16(FP), DX
	BYTE $0x55               // pushq	%rbp
	WORD $0x8948; BYTE $0xe5 // movq	%rsp, %rbp
	LONG $0xf8e48348         // andq	$-8, %rsp
	LONG $0x076ffac5         // vmovdqu	(%rdi), %xmm0
	LONG $0x06d8f9c5         // vpsubusb	(%rsi), %xmm0, %xmm0
	LONG $0x027ffac5         // vmovdqu	%xmm0, (%rdx)
	WORD $0x8948; BYTE $0xec // movq	%rbp, %rsp
	BYTE $0x5d               // popq	%rbp
	BYTE $0xc3               // retq

TEXT ·MAdd256epi8(SB), $0-32
	MOVQ a+0(FP), DI
	MOVQ b+8(FP), SI
	MOVQ out+16(FP), DX
	BYTE $0x55               // pushq	%rbp
	WORD $0x8948; BYTE $0xe5 // movq	%rsp, %rbp
	LONG $0xf8e48348         // andq	$-8, %rsp
	LONG $0x066ffec5         // vmovdqu	(%rsi), %ymm0
	LONG $0x07fcfdc5         // vpaddb	(%rdi), %ymm0, %ymm0
	LONG $0x027ffec5         // vmovdqu	%ymm0, (%rdx)
	WORD $0x8948; BYTE $0xec // movq	%rbp, %rsp
	BYTE $0x5d               // popq	%rbp
	WORD $0xf8c5; BYTE $0x77 // vzeroupper
	BYTE $0xc3               // retq

TEXT ·MSub256epi8(SB), $0-32
	MOVQ a+0(FP), DI
	MOVQ b+8(FP), SI
	MOVQ out+16(FP), DX
	BYTE $0x55               // pushq	%rbp
	WORD $0x8948; BYTE $0xe5 // movq	%rsp, %rbp
	LONG $0xf8e48348         // andq	$-8, %rsp
	LONG $0x076ffec5         // vmovdqu	(%rdi), %ymm0
	LONG $0x06f8fdc5         // vpsubb	(%rsi), %ymm0, %ymm0
	LONG $0x027ffec5         // vmovdqu	%ymm0, (%rdx)
	WORD $0x8948; BYTE $0xec // movq	%rbp, %rsp
	BYTE $0x5d               // popq	%rbp
	WORD $0xf8c5; BYTE $0x77 // vzeroupper
	BYTE $0xc3               // retq

TEXT ·MAdds256epi8(SB), $0-32
	MOVQ a+0(FP), DI
	MOVQ b+8(FP), SI
	MOVQ out+16(FP), DX
	BYTE $0x55               // pushq	%rbp
	WORD $0x8948; BYTE $0xe5 // movq	%rsp, %rbp
	LONG $0xf8e48348         // andq	$-8, %rsp
	LONG $0x076ffec5         // vmovdqu	(%rdi), %ymm0
	LONG $0x06ecfdc5         // vpaddsb	(%rsi), %ymm0, %ymm0
	LONG $0x027ffec5         // vmovdqu	%ymm0, (%rdx)
	WORD $0x8948; BYTE $0xec // movq	%rbp, %rsp
	BYTE $0x5d               // popq	%rbp
	WORD $0xf8c5; BYTE $0x77 // vzeroupper
	BYTE $0xc3               // retq

TEXT ·MSubs256epi8(SB), $0-32
	MOVQ a+0(FP), DI
	MOVQ b+8(FP), SI
	MOVQ out+16(FP), DX
	BYTE $0x55               // pushq	%rbp
	WORD $0x8948; BYTE $0xe5 // movq	%rsp, %rbp
	LONG $0xf8e48348         // andq	$-8, %rsp
	LONG $0x076ffec5         // vmovdqu	(%rdi), %ymm0
	LONG $0x06e8fdc5         // vpsubsb	(%rsi), %ymm0, %ymm0
	LONG $0x027ffec5         // vmovdqu	%ymm0, (%rdx)
	WORD $0x8948; BYTE $0xec // movq	%rbp, %rsp
	BYTE $0x5d               // popq	%rbp
	WORD $0xf8c5; BYTE $0x77 // vzeroupper
	BYTE $0xc3               // retq

TEXT ·MAdds256epu8(SB), $0-32
	MOVQ a+0(FP), DI
	MOVQ b+8(FP), SI
	MOVQ out+16(FP), DX
	BYTE $0x55               // pushq	%rbp
	WORD $0x8948; BYTE $0xe5 // movq	%rsp, %rbp
	LONG $0xf8e48348         // andq	$-8, %rsp
	LONG $0x076ffec5         // vmovdqu	(%rdi), %ymm0
	LONG $0x06dcfdc5         // vpaddusb	(%rsi), %ymm0, %ymm0
	LONG $0x027ffec5         // vmovdqu	%ymm0, (%rdx)
	WORD $0x8948; BYTE $0xec // movq	%rbp, %rsp
	BYTE $0x5d               // popq	%rbp
	WORD $0xf8c5; BYTE $0x77 // vzeroupper
	BYTE $0xc3               // retq

TEXT ·MSubs256epu8(SB), $0-32
	MOVQ a+0(FP), DI
	MOVQ b+8(FP), SI
	MOVQ out+16(FP), DX
	BYTE $0x55               // pushq	%rbp
	WORD $0x8948; BYTE $0xe5 // movq	%rsp, %rbp
	LONG $0xf8e48348         // andq	$-8, %rsp
	LONG $0x076ffec5         // vmovdqu	(%rdi), %ymm0
	LONG $0x06d8fdc5         // vpsubusb	(%rsi), %ymm0, %ymm0
	LONG $0x027ffec5         // vmovdqu	%ymm0, (%rdx)
	WORD $0x8948; BYTE $0xec // movq	%rbp, %rsp
	BYTE $0x5d               // popq	%rbp
	WORD $0xf8c5; BYTE $0x77 // vzeroupper
	BYTE $0xc3               // retq

TEXT ·MSubs256epu16(SB), $0-32
	MOVQ a+0(FP), DI
	MOVQ b+8(FP), SI
	MOVQ out+16(FP), DX
	BYTE $0x55               // pushq	%rbp
	WORD $0x8948; BYTE $0xe5 // movq	%rsp, %rbp
	LONG $0xf8e48348         // andq	$-8, %rsp
	LONG $0x076ffec5         // vmovdqu	(%rdi), %ymm0
	LONG $0x06d9fdc5         // vpsubusw	(%rsi), %ymm0, %ymm0
	LONG $0x027ffec5         // vmovdqu	%ymm0, (%rdx)
	WORD $0x8948; BYTE $0xec // movq	%rbp, %rsp
	BYTE $0x5d               // popq	%rbp
	WORD $0xf8c5; BYTE $0x77 // vzeroupper
	BYTE $0xc3               // retq
