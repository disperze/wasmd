package types

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"reflect"

	wasmvmtypes "github.com/CosmWasm/wasmvm/types"
	codectypes "github.com/cosmos/cosmos-sdk/codec/types"
	sdk "github.com/cosmos/cosmos-sdk/types"
	sdkerrors "github.com/cosmos/cosmos-sdk/types/errors"
	authztypes "github.com/cosmos/cosmos-sdk/x/authz"
	"github.com/gogo/protobuf/proto"
)

const (
	defaultMemoryCacheSize    uint32 = 100 // in MiB
	defaultSmartQueryGasLimit uint64 = 3_000_000
	defaultContractDebugMode         = false

	// ContractAddrLen defines a valid address length for contracts
	ContractAddrLen = 32
	// SDKAddrLen defines a valid address length that was used in sdk address generation
	SDKAddrLen = 20
)

var (
	_ authztypes.Authorization = &ExecuteContractAuthorization{}
)

func NewAllowedContract(contractAddr sdk.AccAddress, allowedMessages []string) *AllowedContract {
	return &AllowedContract{
		ContractAddress: contractAddr.String(),
		AllowedMessages: allowedMessages,
	}
}

// NewExecuteContractAuthorization creates a new ExecuteContractAuthorization object.
func NewExecuteContractAuthorization(allowedContracts []*AllowedContract) *ExecuteContractAuthorization {
	return &ExecuteContractAuthorization{
		AllowedContracts: allowedContracts,
	}
}

// MsgTypeURL implements Authorization.MsgTypeURL.
func (a ExecuteContractAuthorization) MsgTypeURL() string {
	return "/cosmos.wasm.base.MsgExecuteContract"
}

// Accept implements Authorization.Accept.
func (a ExecuteContractAuthorization) Accept(ctx sdk.Context, msg sdk.Msg) (authztypes.AcceptResponse, error) {
	exec, ok := msg.(*MsgExecuteContract)
	if !ok {
		return authztypes.AcceptResponse{}, sdkerrors.ErrInvalidType.Wrap("type mismatch")
	}

	var allowed *AllowedContract
	for _, allowedContract := range a.AllowedContracts {
		if allowedContract.ContractAddress == exec.Contract {
			allowed = allowedContract
			break
		}
	}

	if allowed == nil {
		return authztypes.AcceptResponse{}, sdkerrors.ErrUnauthorized.Wrapf("cannot run %s contract", exec.Contract)
	}

	err := IsJSONObjectWithTopLevelKey(exec.Msg, allowed.AllowedMessages)
	if err != nil {
		return authztypes.AcceptResponse{}, sdkerrors.ErrUnauthorized.Wrapf("no allowed msg: %s", err.Error())
	}

	if allowed.Once {
		return authztypes.AcceptResponse{Accept: true, Delete: true}, nil
	}

	return authztypes.AcceptResponse{Accept: true}, nil
	update := []*AllowedContract{}
	for _, allowedContract := range a.AllowedContracts {
		if allowedContract.ContractAddress == exec.Contract {
			// if the allowed contract contains the contract address and there aren't allowed messages,
			// then we accept the message.
			if len(allowedContract.AllowedMessages) == 0 && !allowedContract.Once {
				// if the permission is for multiple executions, we accept the message and keep
				// it in the update array
				update = append(update, allowedContract)
				continue
			} else if len(allowedContract.AllowedMessages) > 0 {
				// otherwise we exclude it from the update array
				continue
			}

			// NOTE: exec.Msg is base64 encoded, so we need to decode it first
			// If there are allowedMessages then decode the call and check if the message is allowed
			callBz := []byte{}
			if _, err := base64.RawStdEncoding.Decode(exec.Msg.Bytes(), callBz); err != nil {
				return authztypes.AcceptResponse{}, sdkerrors.Wrap(err, "failed to decode base64")
			}

			messageName := map[string]interface{}{}
			if err := json.Unmarshal(callBz, &messageName); err != nil {
				return authztypes.AcceptResponse{}, sdkerrors.Wrap(err, "failed to unmarshal json")
			}

			for _, allowedFunction := range allowedContract.AllowedMessages {
				if len(messageName) != 1 {
					return authztypes.AcceptResponse{}, sdkerrors.ErrInvalidRequest.Wrap("too many message calls in the same transaction")
				}
				for k := range messageName {
					if allowedFunction == k {
						if allowedContract.Once {
							continue
						} else {
							update = append(update, allowedContract)
							continue
						}
					}
				}
			}
		} else {
			// if the allowed contract doesn't contain the contract address, we add it to the update array
			update = append(update, allowedContract)
		}
	}
	if len(update) == 0 {
		return authztypes.AcceptResponse{Accept: true, Delete: true}, nil
	}
	return authztypes.AcceptResponse{Accept: true, Updated: NewExecuteContractAuthorization(update)}, nil
}

// ValidateBasic implements Authorization.ValidateBasic.
func (a ExecuteContractAuthorization) ValidateBasic() error {
	for _, allowedContract := range a.AllowedContracts {
		if err := allowedContract.ValidateBasic(); err != nil {
			return sdkerrors.Wrap(err, "allowed contract")
		}
	}
	return nil
}

func (a AllowedContract) ValidateBasic() error {
	if len(a.ContractAddress) == 0 {
		return sdkerrors.Wrap(sdkerrors.ErrInvalidAddress, "contract address cannot be empty")
	}
	if _, err := sdk.AccAddressFromBech32(a.ContractAddress); err != nil {
		return sdkerrors.Wrap(err, "contract address")
	}
	return nil
}

func (m Model) ValidateBasic() error {
	if len(m.Key) == 0 {
		return sdkerrors.Wrap(ErrEmpty, "key")
	}
	return nil
}

func (c CodeInfo) ValidateBasic() error {
	if len(c.CodeHash) == 0 {
		return sdkerrors.Wrap(ErrEmpty, "code hash")
	}
	if _, err := sdk.AccAddressFromBech32(c.Creator); err != nil {
		return sdkerrors.Wrap(err, "creator")
	}
	if err := c.InstantiateConfig.ValidateBasic(); err != nil {
		return sdkerrors.Wrap(err, "instantiate config")
	}
	return nil
}

// NewCodeInfo fills a new CodeInfo struct
func NewCodeInfo(codeHash []byte, creator sdk.AccAddress, instantiatePermission AccessConfig) CodeInfo {
	return CodeInfo{
		CodeHash:          codeHash,
		Creator:           creator.String(),
		InstantiateConfig: instantiatePermission,
	}
}

var AllCodeHistoryTypes = []ContractCodeHistoryOperationType{ContractCodeHistoryOperationTypeGenesis, ContractCodeHistoryOperationTypeInit, ContractCodeHistoryOperationTypeMigrate}

// NewContractInfo creates a new instance of a given WASM contract info
func NewContractInfo(codeID uint64, creator, admin sdk.AccAddress, label string, createdAt *AbsoluteTxPosition) ContractInfo {
	var adminAddr string
	if !admin.Empty() {
		adminAddr = admin.String()
	}
	return ContractInfo{
		CodeID:  codeID,
		Creator: creator.String(),
		Admin:   adminAddr,
		Label:   label,
		Created: createdAt,
	}
}

// validatable is an optional interface that can be implemented by an ContractInfoExtension to enable validation
type validatable interface {
	ValidateBasic() error
}

// ValidateBasic does syntax checks on the data. If an extension is set and has the `ValidateBasic() error` method, then
// the method is called as well. It is recommend to implement `ValidateBasic` so that the data is verified in the setter
// but also in the genesis import process.
func (c *ContractInfo) ValidateBasic() error {
	if c.CodeID == 0 {
		return sdkerrors.Wrap(ErrEmpty, "code id")
	}
	if _, err := sdk.AccAddressFromBech32(c.Creator); err != nil {
		return sdkerrors.Wrap(err, "creator")
	}
	if len(c.Admin) != 0 {
		if _, err := sdk.AccAddressFromBech32(c.Admin); err != nil {
			return sdkerrors.Wrap(err, "admin")
		}
	}
	if err := validateLabel(c.Label); err != nil {
		return sdkerrors.Wrap(err, "label")
	}
	if c.Extension == nil {
		return nil
	}

	e, ok := c.Extension.GetCachedValue().(validatable)
	if !ok {
		return nil
	}
	if err := e.ValidateBasic(); err != nil {
		return sdkerrors.Wrap(err, "extension")
	}
	return nil
}

// SetExtension set new extension data. Calls `ValidateBasic() error` on non nil values when method is implemented by
// the extension.
func (c *ContractInfo) SetExtension(ext ContractInfoExtension) error {
	if ext == nil {
		c.Extension = nil
		return nil
	}
	if e, ok := ext.(validatable); ok {
		if err := e.ValidateBasic(); err != nil {
			return err
		}
	}
	any, err := codectypes.NewAnyWithValue(ext)
	if err != nil {
		return sdkerrors.Wrap(sdkerrors.ErrPackAny, err.Error())
	}

	c.Extension = any
	return nil
}

// ReadExtension copies the extension value to the pointer passed as argument so that there is no need to cast
// For example with a custom extension of type `MyContractDetails` it will look as following:
// 		var d MyContractDetails
//		if err := info.ReadExtension(&d); err != nil {
//			return nil, sdkerrors.Wrap(err, "extension")
//		}
func (c *ContractInfo) ReadExtension(e ContractInfoExtension) error {
	rv := reflect.ValueOf(e)
	if rv.Kind() != reflect.Ptr || rv.IsNil() {
		return sdkerrors.Wrap(sdkerrors.ErrInvalidType, "not a pointer")
	}
	if c.Extension == nil {
		return nil
	}

	cached := c.Extension.GetCachedValue()
	elem := reflect.ValueOf(cached).Elem()
	if !elem.Type().AssignableTo(rv.Elem().Type()) {
		return sdkerrors.Wrapf(sdkerrors.ErrInvalidType, "extension is of type %s but argument of %s", elem.Type(), rv.Elem().Type())
	}
	rv.Elem().Set(elem)
	return nil
}

func (c ContractInfo) InitialHistory(initMsg []byte) ContractCodeHistoryEntry {
	return ContractCodeHistoryEntry{
		Operation: ContractCodeHistoryOperationTypeInit,
		CodeID:    c.CodeID,
		Updated:   c.Created,
		Msg:       initMsg,
	}
}

func (c *ContractInfo) AddMigration(ctx sdk.Context, codeID uint64, msg []byte) ContractCodeHistoryEntry {
	h := ContractCodeHistoryEntry{
		Operation: ContractCodeHistoryOperationTypeMigrate,
		CodeID:    codeID,
		Updated:   NewAbsoluteTxPosition(ctx),
		Msg:       msg,
	}
	c.CodeID = codeID
	return h
}

// ResetFromGenesis resets contracts timestamp and history.
func (c *ContractInfo) ResetFromGenesis(ctx sdk.Context) ContractCodeHistoryEntry {
	c.Created = NewAbsoluteTxPosition(ctx)
	return ContractCodeHistoryEntry{
		Operation: ContractCodeHistoryOperationTypeGenesis,
		CodeID:    c.CodeID,
		Updated:   c.Created,
	}
}

// AdminAddr convert into sdk.AccAddress or nil when not set
func (c *ContractInfo) AdminAddr() sdk.AccAddress {
	if c.Admin == "" {
		return nil
	}
	admin, err := sdk.AccAddressFromBech32(c.Admin)
	if err != nil { // should never happen
		panic(err.Error())
	}
	return admin
}

// ContractInfoExtension defines the extension point for custom data to be stored with a contract info
type ContractInfoExtension interface {
	proto.Message
	String() string
}

var _ codectypes.UnpackInterfacesMessage = &ContractInfo{}

// UnpackInterfaces implements codectypes.UnpackInterfaces
func (c *ContractInfo) UnpackInterfaces(unpacker codectypes.AnyUnpacker) error {
	var details ContractInfoExtension
	if err := unpacker.UnpackAny(c.Extension, &details); err != nil {
		return err
	}
	return codectypes.UnpackInterfaces(details, unpacker)
}

// NewAbsoluteTxPosition gets a block position from the context
func NewAbsoluteTxPosition(ctx sdk.Context) *AbsoluteTxPosition {
	// we must safely handle nil gas meters
	var index uint64
	meter := ctx.BlockGasMeter()
	if meter != nil {
		index = meter.GasConsumed()
	}
	height := ctx.BlockHeight()
	if height < 0 {
		panic(fmt.Sprintf("unsupported height: %d", height))
	}
	return &AbsoluteTxPosition{
		BlockHeight: uint64(height),
		TxIndex:     index,
	}
}

// LessThan can be used to sort
func (a *AbsoluteTxPosition) LessThan(b *AbsoluteTxPosition) bool {
	if a == nil {
		return true
	}
	if b == nil {
		return false
	}
	return a.BlockHeight < b.BlockHeight || (a.BlockHeight == b.BlockHeight && a.TxIndex < b.TxIndex)
}

// AbsoluteTxPositionLen number of elements in byte representation
const AbsoluteTxPositionLen = 16

// Bytes encodes the object into a 16 byte representation with big endian block height and tx index.
func (a *AbsoluteTxPosition) Bytes() []byte {
	if a == nil {
		panic("object must not be nil")
	}
	r := make([]byte, AbsoluteTxPositionLen)
	copy(r[0:], sdk.Uint64ToBigEndian(a.BlockHeight))
	copy(r[8:], sdk.Uint64ToBigEndian(a.TxIndex))
	return r
}

// NewEnv initializes the environment for a contract instance
func NewEnv(ctx sdk.Context, contractAddr sdk.AccAddress) wasmvmtypes.Env {
	// safety checks before casting below
	if ctx.BlockHeight() < 0 {
		panic("Block height must never be negative")
	}
	nano := ctx.BlockTime().UnixNano()
	if nano < 1 {
		panic("Block (unix) time must never be empty or negative ")
	}

	env := wasmvmtypes.Env{
		Block: wasmvmtypes.BlockInfo{
			Height:  uint64(ctx.BlockHeight()),
			Time:    uint64(nano),
			ChainID: ctx.ChainID(),
		},
		Contract: wasmvmtypes.ContractInfo{
			Address: contractAddr.String(),
		},
	}
	if txCounter, ok := TXCounter(ctx); ok {
		env.Transaction = &wasmvmtypes.TransactionInfo{Index: txCounter}
	}
	return env
}

// NewInfo initializes the MessageInfo for a contract instance
func NewInfo(creator sdk.AccAddress, deposit sdk.Coins) wasmvmtypes.MessageInfo {
	return wasmvmtypes.MessageInfo{
		Sender: creator.String(),
		Funds:  NewWasmCoins(deposit),
	}
}

// NewWasmCoins translates between Cosmos SDK coins and Wasm coins
func NewWasmCoins(cosmosCoins sdk.Coins) (wasmCoins []wasmvmtypes.Coin) {
	for _, coin := range cosmosCoins {
		wasmCoin := wasmvmtypes.Coin{
			Denom:  coin.Denom,
			Amount: coin.Amount.String(),
		}
		wasmCoins = append(wasmCoins, wasmCoin)
	}
	return wasmCoins
}

// WasmConfig is the extra config required for wasm
type WasmConfig struct {
	// SimulationGasLimit is the max gas to be used in a tx simulation call.
	// When not set the consensus max block gas is used instead
	SimulationGasLimit *uint64
	// SimulationGasLimit is the max gas to be used in a smart query contract call
	SmartQueryGasLimit uint64
	// MemoryCacheSize in MiB not bytes
	MemoryCacheSize uint32
	// ContractDebugMode log what contract print
	ContractDebugMode bool
}

// DefaultWasmConfig returns the default settings for WasmConfig
func DefaultWasmConfig() WasmConfig {
	return WasmConfig{
		SmartQueryGasLimit: defaultSmartQueryGasLimit,
		MemoryCacheSize:    defaultMemoryCacheSize,
		ContractDebugMode:  defaultContractDebugMode,
	}
}

// VerifyAddressLen ensures that the address matches the expected length
func VerifyAddressLen() func(addr []byte) error {
	return func(addr []byte) error {
		if len(addr) != ContractAddrLen && len(addr) != SDKAddrLen {
			return sdkerrors.ErrInvalidAddress
		}
		return nil
	}
}
