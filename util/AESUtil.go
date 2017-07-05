package util

import (
	"crypto/cipher"
	"crypto/aes"
	"encoding/hex"
)

type Aes struct {
}

//解密
func (this *Aes) CBCDecrypter(key []byte, userid_str string) (strDesc string, err error) {
	//userid,_ :=hex.DecodeString("547409b7086f8be77774f3db148d0451")
	userid,_ :=hex.DecodeString(userid_str)
	block, err := aes.NewCipher(key)
	if err != nil {
		panic(err)
	}

	if len(userid) < aes.BlockSize {
		panic("userid too short")
	}
	//初始化向量
	iv := []byte{0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00}
	// CBC mode always works in whole blocks.
	if len(userid)%aes.BlockSize != 0 {
		panic("userid is not a multiple of the block size")
	}
	mode := cipher.NewCBCDecrypter(block, iv)

	// CryptBlocks可以原地更新
	mode.CryptBlocks(userid, userid)
	//fmt.Println(strings.Trim(string(userid),"\u0005")+ "++")
	return string(userid), nil
}