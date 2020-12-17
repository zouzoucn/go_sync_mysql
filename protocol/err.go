package protocol

type Err struct {
	sequenceId int
	errCode int
	sqlState string
	errorMessage string
}

func (e *Err) GetSequenceId() int{
	return e.sequenceId
}

func (e *Err) SetSequenceId(sequenceId int) {
	e.sequenceId = sequenceId
}

func (e *Err) GetErrCode() int{
	return e.errCode
}

func (e *Err) SetErrCode(errCode int) {
	e.errCode = errCode
}

func (e *Err) GetSqlState() string{
	return e.sqlState
}

func (e *Err) SetSqlState(sqlState string) {
	e.sqlState = sqlState
}

func (e *Err) GetErrorMessage() string{
	return e.errorMessage
}

func (e *Err) SetErrorMessage(errorMessage string) {
	e.errorMessage = errorMessage
}


func NewErr() *Err{
	return &Err{
		sequenceId:   2,
		errCode:      0,
		sqlState:     "HY000",
		errorMessage: "",
	}
}

func (e *Err) getPayload() []byte{
	payload := make([]byte, 0)
	payload = append(payload, Build_byte(byte(ERR))...)
	payload = append(payload, Build_fixed_int(2, e.errCode)...)
	payload = append(payload, Build_byte('#')...)
	payload = append(payload, Build_fixed_str(5, e.sqlState)...)
	payload = append(payload, Build_eof_str(e.errorMessage)...)
	return payload
}

func LoadFromPacket(p *Packet) *Err{
	e := &Err{
		sequenceId:   0,
		errCode:      0,
		sqlState:     "",
		errorMessage: "",
	}
	proto := NewProto(p.ToPacket(), 3)
	e.sequenceId = proto.Get_fixed_int(1)
	proto.Get_filler(1)
	e.errCode = proto.Get_fixed_int(2)
	proto.Get_filler(1)
	e.sqlState = proto.Get_fixed_str(5)
	e.errorMessage = proto.Get_eof_str()
	return e
}
