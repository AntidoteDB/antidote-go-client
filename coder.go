package antidoteclient

import (
    "encoding/binary"
    "io"
    "github.com/golang/protobuf/proto"
    "fmt"
)

func readMsgRaw(reader io.Reader) (data []byte, err error) {
    sizeB := make([]byte, 4)
    var count uint32
    // read the size of the message
    count = 0
    for count != 4 {
        n, err := reader.Read(sizeB[count:])
        if err != nil {
            return
        }
        count += uint32(n)
    }
    sizeI := binary.BigEndian.Uint32(sizeB)
    data = make([]byte, sizeI)
    // read data
    count = 0
    for count != sizeI {
        n, err := reader.Read(data[count:])
        if err != nil {
            data = nil
            return
        }
        count += uint32(n)
    }
    return
}

func (op *ApbReadObjects) encode(writer io.Writer) (err error) {
    return encodeMsg(op, 116, writer)
}

func (op *ApbUpdateObjects) encode(writer io.Writer) (err error) {
    return encodeMsg(op, 118, writer)
}

func (op *ApbStartTransaction) encode(writer io.Writer) (err error) {
    return encodeMsg(op, 119, writer)
}

func (op *ApbAbortTransaction) encode(writer io.Writer) (err error) {
    return encodeMsg(op, 120, writer)
}

func (op *ApbCommitTransaction) encode(writer io.Writer) (err error) {
    return encodeMsg(op, 121, writer)
}

func (op *ApbStaticUpdateObjects) encode(writer io.Writer) (err error) {
    return encodeMsg(op, 122, writer)
}

func (op *ApbStaticReadObjects) encode(writer io.Writer) (err error) {
    return encodeMsg(op, 123, writer)
}

func encodeMsg(message proto.Message, msgCode byte, writer io.Writer) (err error) {
    msg, err := proto.Marshal(message)
    if err != nil {
        return
    }
    msgsize := len(msg)
    buf := make([]byte, 5)
    binary.BigEndian.PutUint32(buf[0:4], uint32(msgsize))
    buf[5] = msgCode
    writer.Write(buf)
    writer.Write(msg)
    return nil
}

func decodeOperationResp(reader io.Reader) (op *ApbOperationResp, err error) {
    data, err := readMsgRaw(reader)
    if err != nil {
        return
    }
    switch data[0] {
    case 0:
        // error
        err = decodeError(data[1:])
        return
    case 111:
        // transaction response
        resp := &ApbOperationResp{}
        err = proto.Unmarshal(data[1:], resp)
        if err != nil {
            return
        }
        op = resp
        return
    }
    err = fmt.Errorf("invalid message code: %d", data[0])
    return
}

func decodeStartTransactionResp(reader io.Reader) (op *ApbStartTransactionResp, err error) {
    data, err := readMsgRaw(reader)
    if err != nil {
        return
    }
    switch data[0] {
    case 0:
        // error
        err = decodeError(data[1:])
        return
    case 124:
        // transaction response
        resp := &ApbStartTransactionResp{}
        err = proto.Unmarshal(data[1:], resp)
        if err != nil {
            return
        }
        op = resp
        return
    }
    err = fmt.Errorf("invalid message code: %d", data[0])
    return
}

func decodeReadObjectsResp(reader io.Reader) (op *ApbReadObjectsResp, err error) {
    data, err := readMsgRaw(reader)
    if err != nil {
        return
    }
    switch data[0] {
    case 0:
        // error
        err = decodeError(data[1:])
        return
    case 126:
        // transaction response
        resp := &ApbReadObjectsResp{}
        err = proto.Unmarshal(data[1:], resp)
        if err != nil {
            return
        }
        op = resp
        return
    }
    err = fmt.Errorf("invalid message code: %d", data[0])
    return
}

func decodeCommitResp(reader io.Reader) (op *ApbCommitResp, err error) {
    data, err := readMsgRaw(reader)
    if err != nil {
        return
    }
    switch data[0] {
    case 0:
        // error
        err = decodeError(data[1:])
        return
    case 127:
        // transaction response
        resp := &ApbCommitResp{}
        err = proto.Unmarshal(data[1:], resp)
        if err != nil {
            return
        }
        op = resp
        return
    }
    err = fmt.Errorf("invalid message code: %d", data[0])
    return
}

func decodeStaticReadObjectsResp(reader io.Reader) (op *ApbStaticReadObjectsResp, err error) {
    data, err := readMsgRaw(reader)
    if err != nil {
        return
    }
    switch data[0] {
    case 0:
        // error
        err = decodeError(data[1:])
        return
    case 128:
        // transaction response
        resp := &ApbStaticReadObjectsResp{}
        err = proto.Unmarshal(data[1:], resp)
        if err != nil {
            return
        }
        op = resp
        return
    }
    err = fmt.Errorf("invalid message code: %d", data[0])
    return
}


func decodeError(data []byte) (err error) {
    resp := &ApbErrorResp{}
    err = proto.Unmarshal(data, resp)
    if err != nil {
        return
    }
    err = fmt.Errorf("antidote error code %d, %s", resp.Errcode, string(resp.Errmsg))
    return
}
