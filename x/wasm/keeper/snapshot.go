package keeper

import (
	"bytes"
	"encoding/binary"
	"fmt"

	"github.com/CosmWasm/wasmd/x/wasm/types"
	"github.com/tendermint/tendermint/libs/log"

	"github.com/cosmos/cosmos-sdk/store/prefix"
	storetypes "github.com/cosmos/cosmos-sdk/store/types"
	sdkerrors "github.com/cosmos/cosmos-sdk/types/errors"
)

type WasmSnapshot struct {
	logger log.Logger
	keeper *Keeper
}

func NewWasmSnapshot(logger log.Logger, keeper *Keeper) *WasmSnapshot {
	return &WasmSnapshot{
		logger: logger,
		keeper: keeper,
	}
}

func (ws WasmSnapshot) GetItems(ctx storetypes.SnapshotContext) ([]storetypes.SnapshotCustomData, error) {
	ws.logger.Info("Wasm Sync: snapshot start")
	items := []storetypes.SnapshotCustomData{}
	wasmstore := ctx.MultiStore().GetKVStore(ws.keeper.storeKey)
	ws.logger.Info("Wasm Sync: store key: " + ws.keeper.storeKey.String())
	prefixStore := prefix.NewStore(wasmstore, types.CodeKeyPrefix)
	iter := prefixStore.Iterator(nil, nil)
	ws.logger.Info("Wasm Sync:Start iteration")
	for ; iter.Valid(); iter.Next() {
		codeID := binary.BigEndian.Uint64(iter.Key())
		ws.logger.Info(fmt.Sprintf("Wasm Sync: found codeID: %d", codeID))
		var c types.CodeInfo
		ws.keeper.cdc.MustUnmarshal(iter.Value(), &c)

		ws.logger.Info("Wasm Sync: parse code creator: " + c.Creator)
		bytecode, err := ws.keeper.wasmVM.GetCode(c.CodeHash)
		if err != nil {
			return nil, err
		}

		ws.logger.Info("Wasm Sync: add wasmcode to items")
		items = append(items, storetypes.SnapshotCustomData{
			Key:   c.CodeHash,
			Value: bytecode,
		})
	}

	return items, nil
}

func (ws WasmSnapshot) RestoreItem(ctx storetypes.SnapshotContext, item *storetypes.SnapshotCustomData) error {
	ws.logger.Info("Wasm Restore: restore item")
	newCodeHash, err := ws.keeper.wasmVM.Create(item.Value)
	if err != nil {
		return sdkerrors.Wrap(types.ErrCreateFailed, err.Error())
	}
	ws.logger.Info("Wasm Restore: saved code, and get hash")
	if !bytes.Equal(item.Key, newCodeHash) {
		return sdkerrors.Wrap(types.ErrInvalid, "code hashes not same")
	}
	ws.logger.Info("Wasm Restore: validated code hash")

	return nil
}
