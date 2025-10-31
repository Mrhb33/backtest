package engine

// Volume and partial fills simulation

type VolumeModel interface {
	GetAvailableVolume(ts uint64, price float64) float64
}

type FixedVolumeModel struct {
	Volume float64
}

func (m FixedVolumeModel) GetAvailableVolume(ts uint64, price float64) float64 {
	return m.Volume
}

type PartialFillConfig struct {
	Enabled    bool
	MaxPartial float64 // max fraction that can be partially filled
	MinFill    float64 // minimum fill size
}

func (c PartialFillConfig) ShouldPartialFill(orderQty, availableQty float64) (bool, float64) {
	if !c.Enabled {
		return false, orderQty
	}

	if availableQty >= orderQty {
		return false, orderQty // full fill
	}

	if availableQty < c.MinFill {
		return false, 0 // no fill
	}

	partialQty := availableQty
	if partialQty > orderQty*c.MaxPartial {
		partialQty = orderQty * c.MaxPartial
	}

	return true, partialQty
}
