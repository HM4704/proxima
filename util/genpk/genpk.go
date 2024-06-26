package main

import (
	"crypto/ed25519"
	"encoding/hex"
	"fmt"
	"math/rand"
	"os"
	"strconv"
	"strings"

	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/lunfardo314/proxima/ledger"
	"github.com/lunfardo314/proxima/util"
	"github.com/lunfardo314/proxima/util/lines"
	"github.com/lunfardo314/proxima/util/testutil"
	"gopkg.in/yaml.v2"
)

const usage = "Usage: genpk <output file name> <number of private keys/addresses to generate>"

func main() {
	if len(os.Args) != 3 {
		fmt.Println(usage)
		os.Exit(1)
	}
	n, err := strconv.Atoi(os.Args[2])
	util.AssertNoError(err)
	util.Assertf(n > 0, "must be a positive number")
	fmt.Printf("FOR TESTING PURPOSES ONLY! DO NOT USE IN PRODUCTION!\nGenerate %d private keys and ED25519 addresses to the file %s.yaml\n", n, os.Args[1])

	privateKeys := testutil.GetTestingPrivateKeys(n, rand.Int())
	addresses := make([]ledger.AddressED25519, len(privateKeys))

	for i := range privateKeys {
		addresses[i] = ledger.AddressED25519FromPrivateKey(privateKeys[i])
	}

	ln := lines.New().
		Add("# This file was generated by 'genpk' program. ").
		Add("# command line: '%s'", strings.Join(os.Args, " ")).
		Add("# FOR TESTING PURPOSES ONLY! DO NOT USE IN PRODUCTION!")

	for i := range privateKeys {
		ln.Add("# --- %d ---", i)
		ln.Add("-")
		ln.Add("   pk: %s", hex.EncodeToString(privateKeys[i]))
		ln.Add("   addr: %s", addresses[i].String())
	}

	fname := os.Args[1] + ".yaml"
	err = os.WriteFile(fname, []byte(ln.String()), 0644)
	util.AssertNoError(err)

	keysBack, err := ReadTestKeys(fname)
	util.AssertNoError(err)

	util.Assertf(len(keysBack) == len(privateKeys), "len(keysBack)==len(privateKeys)")
}

type (
	TestKey struct {
		PrivateKey ed25519.PrivateKey
		PublicKey  ed25519.PublicKey
		HostID     peer.ID
		Address    ledger.AddressED25519
	}

	testKeyYaml struct {
		PrivateKey string `yaml:"pk"`
		Addr       string `yaml:"addr"`
	}
)

func ReadTestKeys(fname string) ([]TestKey, error) {
	yamlData, err := os.ReadFile(fname)
	if err != nil {
		return nil, err
	}

	tmpYaml := make([]testKeyYaml, 0)
	if err = yaml.Unmarshal(yamlData, &tmpYaml); err != nil {
		return nil, err
	}

	ret := make([]TestKey, len(tmpYaml))
	for i, keyData := range tmpYaml {
		pk, err := util.PrivateKeyFromHexString(keyData.PrivateKey)
		if err != nil {
			return nil, fmt.Errorf("wrong private key at pos %d: %v", i, err)
		}
		addr := ledger.AddressED25519FromPrivateKey(pk)
		if addr.String() != keyData.Addr {
			return nil, fmt.Errorf("private key and address does not match at pos %d", i)
		}
		pklpp, err := crypto.UnmarshalEd25519PrivateKey(pk)
		if err != nil {
			return nil, fmt.Errorf("wrong private key according to libp2p at pos %d", i)
		}
		hostID, err := peer.IDFromPrivateKey(pklpp)
		if err != nil {
			return nil, fmt.Errorf("IDFromPrivateKey: according to libp2p at pos %d: %v", i, err)
		}
		ret[i] = TestKey{
			PrivateKey: pk,
			PublicKey:  pk.Public().(ed25519.PublicKey),
			HostID:     hostID,
			Address:    addr,
		}
	}
	return ret, err
}
