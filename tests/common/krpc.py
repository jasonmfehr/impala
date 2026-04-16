# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

from __future__ import absolute_import, division, print_function

import socket
import ssl
import struct


# Exports a single function for validating KRPC TLS support on a server.
# See the function-level documentation on the "krpc_tls_validate" function for details.
#
# TODO: IMPALA-14907: Python's ssl module does not currently support limiting the allowed
#                     list of TLS 1.3 ciphersuites. Thus, this code cannot currently
#                     validate that any TLS 1.3 ciphersuite is not supported.

_KRPC_MAGIC = b"hrpc"
_KRPC_VERSION = 9
_KRPC_NEGOTIATE_CALL_ID = -33

_STEP_NEGOTIATE = 1
_STEP_TLS_HANDSHAKE = 5

_FEATURE_TLS = 2

_WIRE_VARINT = 0
_WIRE_LEN = 2


def _encode_varint(value):
  if value < 0:
    raise ValueError("varint value must be non-negative")
  out = bytearray()
  while value >= 0x80:
    out.append((value & 0x7f) | 0x80)
    value >>= 7
  out.append(value)
  return bytes(out)


def _decode_varint(buf, offset):
  value = 0
  shift = 0
  while True:
    if offset >= len(buf):
      raise ValueError("truncated varint")
    byte = buf[offset]
    if not isinstance(byte, int):
      byte = ord(byte)
    offset += 1
    value |= (byte & 0x7f) << shift
    if (byte & 0x80) == 0:
      return value, offset
    shift += 7
    if shift > 63:
      raise ValueError("varint too long")


def _field_key(field_number, wire_type):
  return _encode_varint((field_number << 3) | wire_type)


def _encode_varint_field(field_number, value):
  return _field_key(field_number, _WIRE_VARINT) + _encode_varint(value)


def _encode_len_field(field_number, value):
  return _field_key(field_number, _WIRE_LEN) + _encode_varint(len(value)) + value


def _int32_to_wire(value):
  if value < 0:
    return value + (1 << 64)
  return value


def _wire_to_int32(value):
  value &= 0xffffffff
  if value >= 0x80000000:
    return value - 0x100000000
  return value


def _skip_field(buf, offset, wire_type):
  if wire_type == _WIRE_VARINT:
    _, offset = _decode_varint(buf, offset)
    return offset
  if wire_type == _WIRE_LEN:
    size, offset = _decode_varint(buf, offset)
    end = offset + size
    if end > len(buf):
      raise ValueError("truncated length-delimited field")
    return end
  raise ValueError("unsupported protobuf wire type {}".format(wire_type))


def _encode_request_header(call_id):
  return _encode_varint_field(3, _int32_to_wire(call_id))


def _encode_authn_type_sasl():
  # AuthenticationTypePB { sasl = {} }.
  authn = _encode_len_field(1, b"")
  return _encode_len_field(7, authn)


def _encode_negotiate(step, supported_features=None, tls_handshake=None):
  parts = []
  if supported_features:
    for feature in supported_features:
      parts.append(_encode_varint_field(1, feature))
  parts.append(_encode_varint_field(2, step))
  if tls_handshake is not None:
    parts.append(_encode_len_field(5, tls_handshake))
  if step == _STEP_NEGOTIATE:
    parts.append(_encode_authn_type_sasl())
  return b"".join(parts)


def _parse_response_header(header_bytes):
  call_id = None
  is_error = False
  offset = 0
  while offset < len(header_bytes):
    key, offset = _decode_varint(header_bytes, offset)
    field_number = key >> 3
    wire_type = key & 0x7
    if field_number == 1 and wire_type == _WIRE_VARINT:
      raw, offset = _decode_varint(header_bytes, offset)
      call_id = _wire_to_int32(raw)
    elif field_number == 2 and wire_type == _WIRE_VARINT:
      raw, offset = _decode_varint(header_bytes, offset)
      is_error = raw != 0
    else:
      offset = _skip_field(header_bytes, offset, wire_type)
  if call_id is None:
    raise ValueError("ResponseHeader missing call_id")
  return call_id, is_error


def _parse_negotiate(body_bytes):
  parsed = {
      "step": None,
      "supported_features": [],
      "has_tls_handshake": False,
      "tls_handshake": b"",
  }
  offset = 0
  while offset < len(body_bytes):
    key, offset = _decode_varint(body_bytes, offset)
    field_number = key >> 3
    wire_type = key & 0x7
    if field_number == 1 and wire_type == _WIRE_VARINT:
      feature, offset = _decode_varint(body_bytes, offset)
      parsed["supported_features"].append(feature)
    elif field_number == 2 and wire_type == _WIRE_VARINT:
      step, offset = _decode_varint(body_bytes, offset)
      parsed["step"] = step
    elif field_number == 5 and wire_type == _WIRE_LEN:
      size, offset = _decode_varint(body_bytes, offset)
      end = offset + size
      if end > len(body_bytes):
        raise ValueError("truncated tls_handshake field")
      parsed["has_tls_handshake"] = True
      parsed["tls_handshake"] = body_bytes[offset:end]
      offset = end
    else:
      offset = _skip_field(body_bytes, offset, wire_type)

  if parsed["step"] is None:
    raise ValueError("NegotiatePB missing required step")
  return parsed


def _recv_exact(sock, num_bytes):
  out = bytearray()
  while len(out) < num_bytes:
    chunk = sock.recv(num_bytes - len(out))
    if not chunk:
      raise RuntimeError("Connection closed while reading {} bytes".format(num_bytes))
    out.extend(chunk)
  return bytes(out)


def _send_frame(sock, header_bytes, body_bytes):
  frame = _encode_varint(len(header_bytes)) + header_bytes + \
      _encode_varint(len(body_bytes)) + body_bytes
  sock.sendall(struct.pack(">I", len(frame)) + frame)


def _recv_frame(sock):
  total_size = struct.unpack(">I", _recv_exact(sock, 4))[0]
  payload = _recv_exact(sock, total_size)

  offset = 0
  header_size, offset = _decode_varint(payload, offset)
  header_end = offset + header_size
  if header_end > len(payload):
    raise ValueError("truncated framed header")
  header_bytes = payload[offset:header_end]
  offset = header_end

  body_size, offset = _decode_varint(payload, offset)
  body_end = offset + body_size
  if body_end != len(payload):
    raise ValueError("invalid framed body size")
  body_bytes = payload[offset:body_end]
  return header_bytes, body_bytes


def _send_krpc_connection_header(sock):
  sock.sendall(_KRPC_MAGIC + struct.pack("BBB", _KRPC_VERSION, 0, 0))


def _drain_outgoing_bio(outgoing_bio):
  chunks = []
  while True:
    data = outgoing_bio.read()
    if not data:
      break
    chunks.append(data)
  if not chunks:
    return b""
  return b"".join(chunks)


def _advance_tls_handshake(ssl_obj, outgoing_bio):
  while True:
    try:
      ssl_obj.do_handshake()
      return True, _drain_outgoing_bio(outgoing_bio)
    except ssl.SSLWantReadError:
      return False, _drain_outgoing_bio(outgoing_bio)
    except ssl.SSLWantWriteError:
      token = _drain_outgoing_bio(outgoing_bio)
      if token:
        return False, token
      raise RuntimeError("TLS handshake requested write but produced no token")
    except ssl.SSLError:
      return None, b""


def _validate_single_ciphersuite(ciphersuite):
  if not isinstance(ciphersuite, str):
    raise TypeError("ciphersuite must be a string")
  ciphersuite = ciphersuite.strip()
  if not ciphersuite:
    raise ValueError("ciphersuite must be non-empty")
  # OpenSSL uses ':' (and sometimes ',') to separate multiple ciphersuites.
  if ":" in ciphersuite or "," in ciphersuite:
    raise ValueError("Exactly one ciphersuite must be provided")
  return ciphersuite


def _build_context(tls_version, ciphersuite):
  if hasattr(ssl, "PROTOCOL_TLS_CLIENT"):
    context = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
  else:
    context = ssl.SSLContext(ssl.PROTOCOL_TLS)

  context.check_hostname = False
  context.verify_mode = ssl.CERT_NONE

  if not hasattr(ssl, "TLSVersion"):
    raise RuntimeError("TLS version pinning requires ssl.TLSVersion support")

  if tls_version == "1.0":
    context.minimum_version = ssl.TLSVersion.TLSv1
    context.maximum_version = ssl.TLSVersion.TLSv1
    context.set_ciphers(ciphersuite)
  elif tls_version == "1.1":
    context.minimum_version = ssl.TLSVersion.TLSv1_1
    context.maximum_version = ssl.TLSVersion.TLSv1_1
    context.set_ciphers(ciphersuite)
  elif tls_version == "1.2":
    context.minimum_version = ssl.TLSVersion.TLSv1_2
    context.maximum_version = ssl.TLSVersion.TLSv1_2
    context.set_ciphers(ciphersuite)
  elif tls_version == "1.3":
    context.minimum_version = ssl.TLSVersion.TLSv1_3
    context.maximum_version = ssl.TLSVersion.TLSv1_3
  else:
    raise AssertionError("Unhandled TLS version: {}".format(tls_version))

  return context


def krpc_tls_validate(host, port, tls_version, ciphersuite):
  """
     Validates whether the server at the given host and port supports KRPC TLS connections
     using the specified TLS version and ciphersuite. Returns True if the connection and
     TLS handshake succeed, False if the server does not support KRPC TLS or if the TLS
     handshake fails, and raises an exception for other errors such as network issues or
     invalid arguments.

     Note: A False return value does not necessarily mean the server does not support TLS
     but rather that it does not support TLS with the given version and ciphersuite.

     Note: Because of limitations in Python's ssl module, this function cannot validate
           that any TLS 1.3 ciphersuite is not supported. Thus, when tls_version is "1.3",
           the ciphersuite parameter's value is ignored (IMPALA-14907).

     The function performs a full KRPC negotiate and TLS handshake, so it may take several
     seconds to complete.

     Args:
         host: The server hostname or IP address as a string.
         port: The server port as an integer.
         tls_version: The TLS version to use as a string, must be "1.2" or "1.3".
         ciphersuite: One TLS ciphersuite to use as a string
                      (e.g. "TLS_AES_128_GCM_SHA256"). Must specify only one ciphersuite.
                      Ignored if tls_version is "1.3".

     Returns:
         True: If the server supports KRPC TLS with the specified version and ciphersuite
               and the TLS handshake succeeds.
         False: If the server does not support KRPC TLS with the specified version and
                ciphersuite or if the TLS handshake fails.

     Raises:
         ValueError for invalid arguments
         RuntimeError for unexpected responses or network issues.
  """
  if not isinstance(host, str) or not host.strip():
    raise ValueError("host must be a non-empty string")

  assert tls_version == "1.0" or tls_version == "1.1" or tls_version == "1.2" \
      or tls_version == "1.3", "tls_version must be '1.0', '1.1', '1.2', or '1.3'"

  ciphersuite = _validate_single_ciphersuite(ciphersuite)

  try:
    port = int(port)
  except (TypeError, ValueError):
    raise ValueError("port must be an integer")
  if port < 1 or port > 65535:
    raise ValueError("port must be in range [1, 65535]")

  context = _build_context(tls_version, ciphersuite)

  try:
    with socket.create_connection((host, port), timeout=5.0) as raw_sock:
      raw_sock.settimeout(5.0)
      _send_krpc_connection_header(raw_sock)

      negotiate_body = _encode_negotiate(
          _STEP_NEGOTIATE,
          supported_features=[_FEATURE_TLS],
          tls_handshake=None)
      _send_frame(raw_sock,
                  _encode_request_header(_KRPC_NEGOTIATE_CALL_ID),
                  negotiate_body)

      header_bytes, body_bytes = _recv_frame(raw_sock)
      call_id, is_error = _parse_response_header(header_bytes)
      if call_id != _KRPC_NEGOTIATE_CALL_ID:
        raise RuntimeError("Unexpected call_id in negotiate response: {}".format(call_id))
      if is_error:
        raise RuntimeError("Server returned an error response during NEGOTIATE")

      negotiate_resp = _parse_negotiate(body_bytes)
      if negotiate_resp["step"] != _STEP_NEGOTIATE:
        raise RuntimeError(
            "Expected NEGOTIATE step, got {}".format(negotiate_resp["step"]))
      if _FEATURE_TLS not in set(negotiate_resp["supported_features"]):
        return False

      incoming_bio = ssl.MemoryBIO()
      outgoing_bio = ssl.MemoryBIO()
      ssl_obj = context.wrap_bio(incoming_bio, outgoing_bio, server_hostname=host)

      handshake_done, client_token = _advance_tls_handshake(ssl_obj, outgoing_bio)
      if handshake_done is None:
        return False
      if not client_token:
        client_token = b""

      while True:
        tls_req = _encode_negotiate(
            _STEP_TLS_HANDSHAKE,
            supported_features=None,
            tls_handshake=client_token)
        _send_frame(raw_sock,
                    _encode_request_header(_KRPC_NEGOTIATE_CALL_ID),
                    tls_req)

        if handshake_done:
          return True

        header_bytes, body_bytes = _recv_frame(raw_sock)
        call_id, is_error = _parse_response_header(header_bytes)
        if call_id != _KRPC_NEGOTIATE_CALL_ID:
          raise RuntimeError(
              "Unexpected call_id in TLS_HANDSHAKE response: {}".format(call_id))
        if is_error:
          return False

        tls_resp = _parse_negotiate(body_bytes)
        if tls_resp["step"] != _STEP_TLS_HANDSHAKE:
          raise RuntimeError(
              "Expected TLS_HANDSHAKE step, got {}".format(tls_resp["step"]))
        if not tls_resp["has_tls_handshake"]:
          raise RuntimeError("TLS_HANDSHAKE response missing tls_handshake token")

        incoming_bio.write(tls_resp["tls_handshake"])
        handshake_done, client_token = _advance_tls_handshake(ssl_obj, outgoing_bio)
        if handshake_done is None:
          return False
  except ssl.SSLError:
    return False
