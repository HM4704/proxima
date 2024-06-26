package ledger

import (
	"bytes"
	"crypto/ed25519"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"math"
	"time"

	"github.com/lunfardo314/proxima/util"
	"github.com/lunfardo314/proxima/util/lines"
	"golang.org/x/crypto/blake2b"
	"gopkg.in/yaml.v2"
)

// IdentityData is provided at genesis and will remain immutable during lifetime
// All integers are serialized as big-endian
type (
	IdentityData struct {
		// arbitrary string up 255 bytes
		Description string
		// genesis time unix seconds
		GenesisTimeUnix uint32
		// initial supply of tokens
		InitialSupply uint64
		// ED25519 public key of the controller
		GenesisControllerPublicKey ed25519.PublicKey
		// time tick duration in nanoseconds
		TickDuration time.Duration
		// max time tick value in the slot. Up to 256 time ticks per time slot, default 100
		MaxTickValueInSlot uint8
		// ----------- inflation-related
		// NumHalvingEpochs number of halving epochs
		NumHalvingEpochs byte
		// Default 2_289_600. approx one year in slots.
		SlotsPerHalvingEpoch uint32
		// IBranchBonusBase inflation bonus
		BranchBonusBase uint64
		// ChainInflationPerTickFractionBase and HalvingYears
		ChainInflationPerTickFractionBase uint64
		// ChainInflationOpportunitySlots maximum gap between chain outputs for the non-zero inflation
		ChainInflationOpportunitySlots uint64
		// VBCost
		VBCost uint64
		// number of ticks between non-sequencer transactions
		TransactionPace byte
		// number of ticks between sequencer transactions
		TransactionPaceSequencer byte
		// this limits number of sequencers in the network. Reasonable amount would be few hundreds of sequencers
		MinimumAmountOnSequencer uint64
		// limit maximum number of endorsements. For determinism
		MaxNumberOfEndorsements uint64
	}

	// IdentityDataYAMLAble structure for canonical YAMLAble marshaling
	IdentityDataYAMLAble struct {
		GenesisTimeUnix                   uint32 `yaml:"genesis_time_unix"`
		InitialSupply                     uint64 `yaml:"initial_supply"`
		GenesisControllerPublicKey        string `yaml:"genesis_controller_public_key"`
		TimeTickDurationNanosec           int64  `yaml:"time_tick_duration_nanosec"`
		MaxTimeTickValueInTimeSlot        uint8  `yaml:"max_time_tick_value_in_time_slot"`
		BranchBonusBase                   uint64 `yaml:"branch_bonus_base"`
		SlotsPerHalvingEpoch              uint32 `yaml:"slots_per_halving_epoch"`
		VBCost                            uint64 `yaml:"vb_cost"`
		TransactionPace                   byte   `yaml:"transaction_pace"`
		TransactionPaceSequencer          byte   `yaml:"transaction_pace_sequencer"`
		NumHalvingEpochs                  byte   `yaml:"num_halving_epochs"`
		ChainInflationPerTickFractionBase uint64 `yaml:"chain_inflation_per_tick_fraction_base"`
		ChainInflationOpportunitySlots    uint64 `yaml:"chain_inflation_opportunity_slots"`
		MinimumAmountOnSequencer          uint64 `yaml:"minimum_amount_on_sequencer"`
		MaxNumberOfEndorsements           uint64 `yaml:"max_number_of_endorsements"`
		Description                       string `yaml:"description"`
		// non-persistent, for control
		GenesisControllerAddress string `yaml:"genesis_controller_address"`
		BootstrapChainID         string `yaml:"bootstrap_chain_id"`
	}
)

const (
	GenesisOutputIndex     = byte(0)
	GenesisStemOutputIndex = byte(1)
)

func (id *IdentityData) Bytes() []byte {
	var buf bytes.Buffer
	_ = binary.Write(&buf, binary.BigEndian, id.GenesisTimeUnix)
	util.Assertf(len(id.GenesisControllerPublicKey) == ed25519.PublicKeySize, "id.GenesisControllerPublicKey)==ed25519.PublicKeySize")
	buf.Write(id.GenesisControllerPublicKey)
	_ = binary.Write(&buf, binary.BigEndian, id.InitialSupply)
	_ = binary.Write(&buf, binary.BigEndian, id.TickDuration.Nanoseconds())
	_ = binary.Write(&buf, binary.BigEndian, id.MaxTickValueInSlot)
	_ = binary.Write(&buf, binary.BigEndian, id.SlotsPerHalvingEpoch)
	_ = binary.Write(&buf, binary.BigEndian, id.BranchBonusBase)
	_ = binary.Write(&buf, binary.BigEndian, id.NumHalvingEpochs)
	_ = binary.Write(&buf, binary.BigEndian, id.ChainInflationPerTickFractionBase)
	_ = binary.Write(&buf, binary.BigEndian, id.ChainInflationOpportunitySlots)
	_ = binary.Write(&buf, binary.BigEndian, id.VBCost)
	_ = binary.Write(&buf, binary.BigEndian, id.TransactionPace)
	_ = binary.Write(&buf, binary.BigEndian, id.TransactionPaceSequencer)
	_ = binary.Write(&buf, binary.BigEndian, id.MinimumAmountOnSequencer)
	_ = binary.Write(&buf, binary.BigEndian, id.MaxNumberOfEndorsements)
	_ = binary.Write(&buf, binary.BigEndian, uint16(len(id.Description)))
	buf.Write([]byte(id.Description))

	return buf.Bytes()
}

func MustLedgerIdentityDataFromBytes(data []byte) *IdentityData {
	ret := &IdentityData{}
	rdr := bytes.NewReader(data)

	var size16 uint16
	var n int

	err := binary.Read(rdr, binary.BigEndian, &ret.GenesisTimeUnix)
	util.AssertNoError(err)

	buf := make([]byte, ed25519.PublicKeySize)
	n, err = rdr.Read(buf)
	util.AssertNoError(err)
	util.Assertf(n == ed25519.PublicKeySize, "wrong data size")
	ret.GenesisControllerPublicKey = buf

	err = binary.Read(rdr, binary.BigEndian, &ret.InitialSupply)
	util.AssertNoError(err)

	var bufNano int64
	err = binary.Read(rdr, binary.BigEndian, &bufNano)
	util.AssertNoError(err)
	ret.TickDuration = time.Duration(bufNano)

	err = binary.Read(rdr, binary.BigEndian, &ret.MaxTickValueInSlot)
	util.AssertNoError(err)

	err = binary.Read(rdr, binary.BigEndian, &ret.SlotsPerHalvingEpoch)
	util.AssertNoError(err)

	err = binary.Read(rdr, binary.BigEndian, &ret.BranchBonusBase)
	util.AssertNoError(err)

	err = binary.Read(rdr, binary.BigEndian, &ret.NumHalvingEpochs)
	util.AssertNoError(err)

	err = binary.Read(rdr, binary.BigEndian, &ret.ChainInflationPerTickFractionBase)
	util.AssertNoError(err)

	err = binary.Read(rdr, binary.BigEndian, &ret.ChainInflationOpportunitySlots)
	util.AssertNoError(err)

	err = binary.Read(rdr, binary.BigEndian, &ret.VBCost)
	util.AssertNoError(err)

	err = binary.Read(rdr, binary.BigEndian, &ret.TransactionPace)
	util.AssertNoError(err)

	err = binary.Read(rdr, binary.BigEndian, &ret.TransactionPaceSequencer)
	util.AssertNoError(err)

	err = binary.Read(rdr, binary.BigEndian, &ret.MinimumAmountOnSequencer)
	util.AssertNoError(err)

	err = binary.Read(rdr, binary.BigEndian, &ret.MaxNumberOfEndorsements)
	util.AssertNoError(err)

	err = binary.Read(rdr, binary.BigEndian, &size16)
	util.AssertNoError(err)
	buf = make([]byte, size16)
	n, err = rdr.Read(buf)
	util.AssertNoError(err)
	util.Assertf(n == int(size16), "wrong data size")
	ret.Description = string(buf)

	util.Assertf(rdr.Len() == 0, "not all bytes has been read")
	return ret
}

func (id *IdentityData) GenesisTime() time.Time {
	return time.Unix(int64(id.GenesisTimeUnix), 0)
}

func (id *IdentityData) GenesisTimeUnixNano() int64 {
	return time.Unix(int64(id.GenesisTimeUnix), 0).UnixNano()
}

func (id *IdentityData) Hash() [32]byte {
	return blake2b.Sum256(id.Bytes())
}

func (id *IdentityData) GenesisControlledAddress() AddressED25519 {
	return AddressED25519FromPublicKey(id.GenesisControllerPublicKey)
}

func (id *IdentityData) TimeFromRealTime(nowis time.Time) Time {
	util.Assertf(!nowis.Before(id.GenesisTime()), "!nowis.Before(id.GenesisTimeUnix)")
	sinceGenesisNano := nowis.UnixNano() - id.GenesisTimeUnixNano()
	totalTicks := sinceGenesisNano / int64(id.TickDuration)
	tps := int64(id.TicksPerSlot())
	slot := totalTicks / tps
	ticks := totalTicks % tps
	return MustNewLedgerTime(Slot(uint32(slot)), Tick(byte(ticks)))
}

func (id *IdentityData) SlotDuration() time.Duration {
	return id.TickDuration * time.Duration(id.TicksPerSlot())
}

func (id *IdentityData) TicksPerSlot() int {
	return int(id.MaxTickValueInSlot) + 1
}

func (id *IdentityData) OriginChainID() ChainID {
	oid := GenesisOutputID()
	return MakeOriginChainID(&oid)
}

func (id *IdentityData) String() string {
	return string(id.YAMLAble().YAML())
}

func (id *IdentityData) Lines(prefix ...string) *lines.Lines {
	originChainID := id.OriginChainID()
	return lines.New(prefix...).
		Add("Genesis time: %s (Unix %d)", id.GenesisTime().Format(time.RFC3339), id.GenesisTimeUnix).
		Add("Time tick duration: %v", id.TickDuration).
		Add("Time ticks per time slot: %d", id.TicksPerSlot()).
		Add("Transaction pace: %d", id.TransactionPace).
		Add("Sequencer pace: %d", id.TransactionPaceSequencer).
		Add("Description: '%s'", id.Description).
		Add("Initial supply: %s", util.GoTh(id.InitialSupply)).
		Add("Genesis controller address: %s", id.GenesisControlledAddress().String()).
		Add("Origin chain ID: %s", originChainID.String())
}

func (id *IdentityData) YAMLAble() *IdentityDataYAMLAble {
	chainID := id.OriginChainID()
	return &IdentityDataYAMLAble{
		GenesisTimeUnix:                   id.GenesisTimeUnix,
		GenesisControllerPublicKey:        hex.EncodeToString(id.GenesisControllerPublicKey),
		InitialSupply:                     id.InitialSupply,
		TimeTickDurationNanosec:           id.TickDuration.Nanoseconds(),
		MaxTimeTickValueInTimeSlot:        id.MaxTickValueInSlot,
		BranchBonusBase:                   id.BranchBonusBase,
		VBCost:                            id.VBCost,
		TransactionPace:                   id.TransactionPace,
		TransactionPaceSequencer:          id.TransactionPaceSequencer,
		SlotsPerHalvingEpoch:              id.SlotsPerHalvingEpoch,
		NumHalvingEpochs:                  id.NumHalvingEpochs,
		ChainInflationPerTickFractionBase: id.ChainInflationPerTickFractionBase,
		ChainInflationOpportunitySlots:    id.ChainInflationOpportunitySlots,
		GenesisControllerAddress:          id.GenesisControlledAddress().String(),
		MinimumAmountOnSequencer:          id.MinimumAmountOnSequencer,
		MaxNumberOfEndorsements:           id.MaxNumberOfEndorsements,
		BootstrapChainID:                  chainID.StringHex(),
		Description:                       id.Description,
	}
}

func (id *IdentityData) TimeHorizonYears() int {
	return math.MaxUint32 / int(id.SlotsPerHalvingEpoch)
}

func (id *IdentityData) SlotsPerDay() int {
	return int(24 * time.Hour / id.SlotDuration())
}

func (id *IdentityData) TimeConstantsToString() string {
	nowis := time.Now()
	timestampNowis := id.TimeFromRealTime(nowis)

	// TODO assertion sometimes fails due to integer arithmetics at nano level
	util.Assertf(nowis.UnixNano()-timestampNowis.UnixNano() < int64(TickDuration()),
		"nowis.UnixNano()(%d)-timestampNowis.UnixNano()(%d) = %d < int64(TickDuration())(%d)",
		nowis.UnixNano(), timestampNowis.UnixNano(), nowis.UnixNano()-timestampNowis.UnixNano(), int64(TickDuration()))

	maxYears := MaxSlot / (id.SlotsPerDay() * 365)
	return lines.New().
		Add("TickDuration = %v", id.TickDuration).
		Add("TicksPerSlot = %d", id.TicksPerSlot()).
		Add("SlotDuration = %v", id.SlotDuration()).
		Add("SlotsPerDay = %d", id.SlotsPerDay()).
		Add("MaxYears = %d", maxYears).
		Add("TimeHorizonYears = %d", id.TimeHorizonYears()).
		Add("SlotsPerHalvingEpoch = %d", id.SlotsPerHalvingEpoch).
		Add("seconds per year = %d", 60*60*24*365).
		Add("timestamp GenesisTime = %v", id.GenesisTime()).
		Add("nowis %v", nowis).
		Add("nowis nano %d", nowis.UnixNano()).
		Add("GenesisTimeUnix = %d", id.GenesisTimeUnix).
		Add("GenesisTimeUnixNano = %d", id.GenesisTimeUnixNano()).
		Add("timestampNowis = %s ", timestampNowis.String()).
		Add("timestampNowis.Time() = %v ", timestampNowis.Time()).
		Add("timestampNowis.Time().UnixNano() = %v ", timestampNowis.Time().UnixNano()).
		Add("timestampNowis.UnixNano() = %v ", timestampNowis.UnixNano()).
		Add("rounding: nowis.UnixNano() - timestampNowis.UnixNano() = %d", nowis.UnixNano()-timestampNowis.UnixNano()).
		Add("tick duration nano = %d", int64(TickDuration())).
		String()
}

func (id *IdentityData) YAML() []byte {
	return id.YAMLAble().YAML()
}

const stateIDFileComment = `# This is Proxima ledger identity file.
# It contains public Proxima ledger constants set at genesis. 
# The ledger identity file is used to create genesis ledger state for Proxima nodes.
# Private key of the controller should be known only to the creator of the genesis ledger state.
# Public key of the controller identifies originator of the ledger for its lifetime.
# 'genesis_controller_address' is computed from the public key of the controller
# 'bootstrap_chain_id' is a constant, i.e. same for all ledgers
`

func (id *IdentityDataYAMLAble) YAML() []byte {
	var buf bytes.Buffer
	data, err := yaml.Marshal(id)
	util.AssertNoError(err)
	buf.WriteString(stateIDFileComment)
	buf.Write(data)
	return buf.Bytes()
}

func (id *IdentityDataYAMLAble) stateIdentityData() (*IdentityData, error) {
	var err error
	ret := &IdentityData{}
	ret.GenesisTimeUnix = id.GenesisTimeUnix
	ret.GenesisControllerPublicKey, err = hex.DecodeString(id.GenesisControllerPublicKey)
	ret.InitialSupply = id.InitialSupply
	if err != nil {
		return nil, err
	}
	if len(ret.GenesisControllerPublicKey) != ed25519.PublicKeySize {
		return nil, fmt.Errorf("wrong public key")
	}
	ret.TickDuration = time.Duration(id.TimeTickDurationNanosec)
	ret.MaxTickValueInSlot = id.MaxTimeTickValueInTimeSlot
	ret.BranchBonusBase = id.BranchBonusBase
	ret.VBCost = id.VBCost
	ret.TransactionPace = id.TransactionPace
	ret.TransactionPaceSequencer = id.TransactionPaceSequencer
	ret.SlotsPerHalvingEpoch = id.SlotsPerHalvingEpoch
	ret.ChainInflationPerTickFractionBase = id.ChainInflationPerTickFractionBase
	ret.ChainInflationOpportunitySlots = id.ChainInflationOpportunitySlots
	ret.NumHalvingEpochs = id.NumHalvingEpochs
	ret.MinimumAmountOnSequencer = id.MinimumAmountOnSequencer
	ret.MaxNumberOfEndorsements = id.MaxNumberOfEndorsements
	ret.Description = id.Description

	// control
	if AddressED25519FromPublicKey(ret.GenesisControllerPublicKey).String() != id.GenesisControllerAddress {
		return nil, fmt.Errorf("YAML data inconsistency: address and public key does not match")
	}
	chainID := ret.OriginChainID()
	if id.BootstrapChainID != chainID.StringHex() {
		return nil, fmt.Errorf("YAML data inconsistency: bootstrap chain ID does not match")
	}
	return ret, nil
}

func StateIdentityDataFromYAML(yamlData []byte) (*IdentityData, error) {
	yamlAble := &IdentityDataYAMLAble{}
	if err := yaml.Unmarshal(yamlData, &yamlAble); err != nil {
		return nil, err
	}
	return yamlAble.stateIdentityData()
}

func GenesisTransactionID() *TransactionID {
	ret := NewTransactionID(MustNewLedgerTime(0, 0), All0TransactionHash, true)
	return &ret
}

func GenesisOutputID() (ret OutputID) {
	// we are placing sequencer flag = true into the genesis tx ID to please sequencer constraint
	// of the origin branch transaction. It is the only exception
	ret = NewOutputID(GenesisTransactionID(), GenesisOutputIndex)
	return
}

func GenesisStemOutputID() (ret OutputID) {
	ret = NewOutputID(GenesisTransactionID(), GenesisStemOutputIndex)
	return
}
