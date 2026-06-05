//! Order-preserving ("memcomparable") key encoding.
//!
//! The contract every [`OrderedKey`] implementation upholds is:
//!
//! ```text
//! a < b  (logically, via `Ord`)   <=>   encode(a) < encode(b)  (unsigned byte-wise / memcmp)
//! ```
//!
//! This lets rocksmap keep RocksDB's default bytewise comparator while iteration, ranges,
//! and prefix scans follow the logical order of the key type.
//!
//! Encoding summary:
//! - unsigned integers: big-endian, fixed width;
//! - signed integers: big-endian with the sign bit flipped (negatives sort below positives);
//! - `bool`: `0x00`/`0x01`; `char`: big-endian `u32` scalar value;
//! - `String`/`Vec<u8>`: raw bytes, `0x00` escaped as `0x00 0xFF`, terminated by `0x00 0x00`;
//! - `Option<T>`: `0x00` for `None`, `0x01` ++ `encode(T)` for `Some` (so `None < Some(_)`);
//! - tuples: each field in declaration order (fixed-width fields carry no terminator, so a
//!   leading field is a true byte prefix of the whole key — load-bearing for prefix scans);
//! - floats: not on bare `f32`/`f64`; use the explicit [`OrderedF32`]/[`OrderedF64`] wrappers.

use crate::codec::KeyCodec;
use crate::error::{Error, Result};
use std::marker::PhantomData;

/// A key type that can be encoded to bytes such that byte order matches its `Ord` order.
///
/// Encoding writes into a buffer; decoding consumes a prefix of `input`, advancing it so
/// composite types (tuples, `Option`) can decode field-by-field.
pub trait OrderedKey: Sized {
    /// Append the order-preserving encoding of `self` to `out`.
    fn encode_into(&self, out: &mut Vec<u8>);

    /// Decode one value from the front of `input`, advancing `input` past the bytes consumed.
    fn decode_from(input: &mut &[u8]) -> Result<Self>;
}

fn unexpected_end() -> Error {
    Error::Deserialization("unexpected end of ordered key".to_string())
}

fn read_byte(input: &mut &[u8]) -> Result<u8> {
    let (first, rest) = input.split_first().ok_or_else(unexpected_end)?;
    *input = rest;
    Ok(*first)
}

fn read_array<const N: usize>(input: &mut &[u8]) -> Result<[u8; N]> {
    if input.len() < N {
        return Err(unexpected_end());
    }
    let (head, tail) = input.split_at(N);
    let mut arr = [0u8; N];
    arr.copy_from_slice(head);
    *input = tail;
    Ok(arr)
}

/// Escape `0x00` as `0x00 0xFF`, without a terminator. This is the leading portion of a
/// byte-string key's encoding, shared by every key that begins with `bytes` — the basis for
/// prefix scans over `String`/`Vec<u8>` keys.
pub(crate) fn encode_bytes_no_terminator(bytes: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(bytes.len());
    for &b in bytes {
        if b == 0x00 {
            out.push(0x00);
            out.push(0xFF);
        } else {
            out.push(b);
        }
    }
    out
}

/// Encode raw bytes with `0x00` escaping and a `0x00 0x00` terminator. The terminator
/// (`0x00 0x00`) sorts below an escaped zero (`0x00 0xFF`) and below any non-zero byte, so a
/// shorter string sorts before a longer one sharing its prefix.
fn encode_bytes(bytes: &[u8], out: &mut Vec<u8>) {
    out.extend_from_slice(&encode_bytes_no_terminator(bytes));
    out.push(0x00);
    out.push(0x00);
}

fn decode_bytes(input: &mut &[u8]) -> Result<Vec<u8>> {
    let mut out = Vec::new();
    loop {
        let b = read_byte(input)?;
        if b == 0x00 {
            match read_byte(input)? {
                0x00 => return Ok(out), // terminator
                0xFF => out.push(0x00), // escaped zero
                other => {
                    return Err(Error::Deserialization(format!(
                        "invalid escape sequence 0x00 0x{other:02X} in ordered key"
                    )))
                }
            }
        } else {
            out.push(b);
        }
    }
}

macro_rules! impl_unsigned {
    ($($t:ty),+) => {$(
        impl OrderedKey for $t {
            fn encode_into(&self, out: &mut Vec<u8>) {
                out.extend_from_slice(&self.to_be_bytes());
            }
            fn decode_from(input: &mut &[u8]) -> Result<Self> {
                Ok(<$t>::from_be_bytes(read_array::<{ std::mem::size_of::<$t>() }>(input)?))
            }
        }
    )+};
}
impl_unsigned!(u8, u16, u32, u64, u128);

macro_rules! impl_signed {
    ($($t:ty => $u:ty),+) => {$(
        impl OrderedKey for $t {
            fn encode_into(&self, out: &mut Vec<u8>) {
                // Flip the sign bit so negatives (which have it set in two's complement)
                // sort below non-negatives in unsigned byte space.
                let flipped = (*self as $u) ^ (1 as $u).rotate_right(1);
                out.extend_from_slice(&flipped.to_be_bytes());
            }
            fn decode_from(input: &mut &[u8]) -> Result<Self> {
                let bits = <$u>::from_be_bytes(read_array::<{ std::mem::size_of::<$t>() }>(input)?);
                Ok((bits ^ (1 as $u).rotate_right(1)) as $t)
            }
        }
    )+};
}
impl_signed!(i8 => u8, i16 => u16, i32 => u32, i64 => u64, i128 => u128);

// `usize`/`isize` are encoded as fixed 64-bit values for cross-platform determinism.
impl OrderedKey for usize {
    fn encode_into(&self, out: &mut Vec<u8>) {
        (*self as u64).encode_into(out);
    }
    fn decode_from(input: &mut &[u8]) -> Result<Self> {
        let v = u64::decode_from(input)?;
        usize::try_from(v)
            .map_err(|_| Error::Deserialization("usize out of range on this platform".to_string()))
    }
}

impl OrderedKey for isize {
    fn encode_into(&self, out: &mut Vec<u8>) {
        (*self as i64).encode_into(out);
    }
    fn decode_from(input: &mut &[u8]) -> Result<Self> {
        let v = i64::decode_from(input)?;
        isize::try_from(v)
            .map_err(|_| Error::Deserialization("isize out of range on this platform".to_string()))
    }
}

impl OrderedKey for bool {
    fn encode_into(&self, out: &mut Vec<u8>) {
        out.push(if *self { 0x01 } else { 0x00 });
    }
    fn decode_from(input: &mut &[u8]) -> Result<Self> {
        match read_byte(input)? {
            0x00 => Ok(false),
            0x01 => Ok(true),
            other => Err(Error::Deserialization(format!(
                "invalid bool byte 0x{other:02X} in ordered key"
            ))),
        }
    }
}

impl OrderedKey for char {
    fn encode_into(&self, out: &mut Vec<u8>) {
        (*self as u32).encode_into(out);
    }
    fn decode_from(input: &mut &[u8]) -> Result<Self> {
        let scalar = u32::decode_from(input)?;
        char::from_u32(scalar).ok_or_else(|| {
            Error::Deserialization(format!("invalid char scalar 0x{scalar:08X} in ordered key"))
        })
    }
}

impl OrderedKey for String {
    fn encode_into(&self, out: &mut Vec<u8>) {
        encode_bytes(self.as_bytes(), out);
    }
    fn decode_from(input: &mut &[u8]) -> Result<Self> {
        let bytes = decode_bytes(input)?;
        String::from_utf8(bytes)
            .map_err(|e| Error::Deserialization(format!("invalid UTF-8 in ordered key: {e}")))
    }
}

impl OrderedKey for Vec<u8> {
    fn encode_into(&self, out: &mut Vec<u8>) {
        encode_bytes(self, out);
    }
    fn decode_from(input: &mut &[u8]) -> Result<Self> {
        decode_bytes(input)
    }
}

impl<T: OrderedKey> OrderedKey for Option<T> {
    fn encode_into(&self, out: &mut Vec<u8>) {
        match self {
            None => out.push(0x00),
            Some(v) => {
                out.push(0x01);
                v.encode_into(out);
            }
        }
    }
    fn decode_from(input: &mut &[u8]) -> Result<Self> {
        match read_byte(input)? {
            0x00 => Ok(None),
            0x01 => Ok(Some(T::decode_from(input)?)),
            other => Err(Error::Deserialization(format!(
                "invalid Option tag 0x{other:02X} in ordered key"
            ))),
        }
    }
}

macro_rules! impl_tuple {
    ($($name:ident),+) => {
        impl<$($name: OrderedKey),+> OrderedKey for ($($name,)+) {
            #[allow(non_snake_case)]
            fn encode_into(&self, out: &mut Vec<u8>) {
                let ($($name,)+) = self;
                $($name.encode_into(out);)+
            }
            #[allow(non_snake_case)]
            fn decode_from(input: &mut &[u8]) -> Result<Self> {
                Ok(($($name::decode_from(input)?,)+))
            }
        }
    };
}
impl_tuple!(A);
impl_tuple!(A, B);
impl_tuple!(A, B, C);
impl_tuple!(A, B, C, D);
impl_tuple!(A, B, C, D, E);
impl_tuple!(A, B, C, D, E, F);

/// Order-preserving wrapper for `f64` keys.
///
/// `f64` is deliberately *not* an [`OrderedKey`] (it is not `Ord` in std). This wrapper
/// imposes a total order via the standard bit-flip transform, with the documented
/// consequences: `NaN` sorts at an extreme, and `-0.0 < +0.0` (they are distinct keys).
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct OrderedF64(pub f64);

impl OrderedKey for OrderedF64 {
    fn encode_into(&self, out: &mut Vec<u8>) {
        let bits = self.0.to_bits();
        let x = if bits & (1 << 63) != 0 {
            !bits
        } else {
            bits ^ (1 << 63)
        };
        out.extend_from_slice(&x.to_be_bytes());
    }
    fn decode_from(input: &mut &[u8]) -> Result<Self> {
        let x = u64::from_be_bytes(read_array::<8>(input)?);
        let bits = if x & (1 << 63) != 0 {
            x ^ (1 << 63)
        } else {
            !x
        };
        Ok(OrderedF64(f64::from_bits(bits)))
    }
}

/// Order-preserving wrapper for `f32` keys. See [`OrderedF64`] for the semantics.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct OrderedF32(pub f32);

impl OrderedKey for OrderedF32 {
    fn encode_into(&self, out: &mut Vec<u8>) {
        let bits = self.0.to_bits();
        let x = if bits & (1 << 31) != 0 {
            !bits
        } else {
            bits ^ (1 << 31)
        };
        out.extend_from_slice(&x.to_be_bytes());
    }
    fn decode_from(input: &mut &[u8]) -> Result<Self> {
        let x = u32::from_be_bytes(read_array::<4>(input)?);
        let bits = if x & (1 << 31) != 0 {
            x ^ (1 << 31)
        } else {
            !x
        };
        Ok(OrderedF32(f32::from_bits(bits)))
    }
}

/// Order-preserving codec, the default key codec for [`crate::RocksMap`].
///
/// Implements [`KeyCodec`] for any [`OrderedKey`], and is marked [`OrderedKeyCodec`] so that
/// ordered operations (range/prefix scans) can require it at compile time.
pub struct OrderedCodec<T>(PhantomData<T>);

impl<K: OrderedKey> KeyCodec<K> for OrderedCodec<K> {
    fn encode(key: &K) -> Result<Vec<u8>> {
        let mut out = Vec::new();
        key.encode_into(&mut out);
        Ok(out)
    }

    fn decode(bytes: &[u8]) -> Result<K> {
        let mut input = bytes;
        let value = K::decode_from(&mut input)?;
        if !input.is_empty() {
            return Err(Error::Deserialization(
                "trailing bytes after ordered key".to_string(),
            ));
        }
        Ok(value)
    }
}

/// Marker for key codecs whose byte order equals the key's logical (`Ord`) order.
///
/// Implemented by [`OrderedCodec`] but deliberately **not** by `BincodeCodec`, so APIs that
/// depend on ordering (range/prefix scans) can bound on `KC: OrderedKeyCodec<K>`.
pub trait OrderedKeyCodec<K>: KeyCodec<K> {}

impl<K: OrderedKey> OrderedKeyCodec<K> for OrderedCodec<K> {}

/// Byte-string key types that support raw prefix scans (`scan_prefix`).
///
/// Implemented for `String` (`Prefix = str`) and `Vec<u8>` (`Prefix = [u8]`). The encoded
/// prefix is the key body's escaping *without* the terminator, so it is a true byte-prefix of
/// the stored encoding of every key that begins with it.
pub trait PrefixKey: OrderedKey {
    /// The borrowed prefix type (`str` for `String`, `[u8]` for `Vec<u8>`).
    type Prefix: ?Sized;

    /// Encode a prefix to the leading bytes shared by all matching keys' encodings.
    fn encode_prefix(prefix: &Self::Prefix) -> Vec<u8>;
}

impl PrefixKey for String {
    type Prefix = str;
    fn encode_prefix(prefix: &str) -> Vec<u8> {
        encode_bytes_no_terminator(prefix.as_bytes())
    }
}

impl PrefixKey for Vec<u8> {
    type Prefix = [u8];
    fn encode_prefix(prefix: &[u8]) -> Vec<u8> {
        encode_bytes_no_terminator(prefix)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    fn enc<K: OrderedKey>(k: &K) -> Vec<u8> {
        <OrderedCodec<K> as KeyCodec<K>>::encode(k).unwrap()
    }

    fn roundtrip<K: OrderedKey + PartialEq + std::fmt::Debug>(k: K) {
        let bytes = enc(&k);
        let decoded = <OrderedCodec<K> as KeyCodec<K>>::decode(&bytes).unwrap();
        assert_eq!(k, decoded, "round-trip mismatch");
    }

    #[test]
    fn unsigned_order_matches_numeric_across_byte_boundaries() {
        assert!(enc(&1u64) < enc(&256u64));
        assert!(enc(&255u64) < enc(&256u64));
        assert!(enc(&0u64) < enc(&u64::MAX));
        let mut sorted = [1000u64, 2, 256, 1, 0, u64::MAX, 255];
        sorted.sort();
        for w in sorted.windows(2) {
            assert!(enc(&w[0]) < enc(&w[1]));
        }
    }

    #[test]
    fn signed_negatives_sort_below_positives() {
        assert!(enc(&i64::MIN) < enc(&-1i64));
        assert!(enc(&-1i64) < enc(&0i64));
        assert!(enc(&0i64) < enc(&1i64));
        assert!(enc(&1i64) < enc(&i64::MAX));
        assert!(enc(&-100i64) < enc(&-50i64));
    }

    #[test]
    fn string_prefix_sorts_before_longer() {
        assert!(enc(&"a".to_string()) < enc(&"aa".to_string()));
        assert!(enc(&"a".to_string()) < enc(&"b".to_string()));
        assert!(enc(&"".to_string()) < enc(&"a".to_string()));
        // lexicographic, not ordered by length:
        assert!(enc(&"aa".to_string()) < enc(&"b".to_string()));
        // embedded NUL is escaped and still orders correctly:
        assert!(enc(&"a".to_string()) < enc(&"a\u{0}".to_string()));
    }

    #[test]
    fn composite_fields_disambiguate() {
        let a = ("a".to_string(), "bc".to_string());
        let b = ("ab".to_string(), "c".to_string());
        assert_ne!(enc(&a), enc(&b));
        assert!(
            enc(&("ab".to_string(), "".to_string())) < enc(&("ab".to_string(), "c".to_string()))
        );
        // leading-field tuple is a byte prefix of the full key (basis for prefix scans):
        let full = enc(&(7u64, 42u64));
        let prefix = enc(&(7u64,));
        assert!(full.starts_with(&prefix));
    }

    #[test]
    fn option_none_sorts_below_some() {
        assert!(enc(&None::<u64>) < enc(&Some(0u64)));
        assert!(enc(&Some(0u64)) < enc(&Some(1u64)));
    }

    #[test]
    fn float_wrapper_total_order() {
        assert!(enc(&OrderedF64(f64::NEG_INFINITY)) < enc(&OrderedF64(-1.0)));
        assert!(enc(&OrderedF64(-1.0)) < enc(&OrderedF64(0.0)));
        assert!(enc(&OrderedF64(0.0)) < enc(&OrderedF64(1.0)));
        assert!(enc(&OrderedF64(1.0)) < enc(&OrderedF64(f64::INFINITY)));
        // documented: -0.0 sorts below +0.0
        assert!(enc(&OrderedF64(-0.0)) < enc(&OrderedF64(0.0)));
    }

    #[test]
    fn trailing_bytes_rejected() {
        let mut bytes = enc(&5u64);
        bytes.push(0xAB);
        assert!(<OrderedCodec<u64> as KeyCodec<u64>>::decode(&bytes).is_err());
    }

    #[test]
    fn edge_value_roundtrips() {
        roundtrip(0u64);
        roundtrip(u64::MAX);
        roundtrip(i64::MIN);
        roundtrip(i64::MAX);
        roundtrip(0u128);
        roundtrip(u128::MAX);
        roundtrip(i128::MIN);
        roundtrip(true);
        roundtrip('🦀');
        roundtrip(String::new());
        roundtrip("hello\u{0}world".to_string());
        roundtrip(vec![0u8, 1, 2, 0, 255]);
        roundtrip(None::<String>);
        roundtrip(Some(42u32));
        roundtrip((1u64, "x".to_string(), -3i32));
    }

    // --- Property tests: round-trip and order preservation ---

    proptest! {
        #[test]
        fn prop_u64_order(a: u64, b: u64) {
            prop_assert_eq!(a.cmp(&b), enc(&a).cmp(&enc(&b)));
        }

        #[test]
        fn prop_i64_order(a: i64, b: i64) {
            prop_assert_eq!(a.cmp(&b), enc(&a).cmp(&enc(&b)));
        }

        #[test]
        fn prop_i128_order(a: i128, b: i128) {
            prop_assert_eq!(a.cmp(&b), enc(&a).cmp(&enc(&b)));
        }

        #[test]
        fn prop_string_order(a: String, b: String) {
            prop_assert_eq!(a.cmp(&b), enc(&a).cmp(&enc(&b)));
        }

        #[test]
        fn prop_bytes_order(a: Vec<u8>, b: Vec<u8>) {
            prop_assert_eq!(a.cmp(&b), enc(&a).cmp(&enc(&b)));
        }

        #[test]
        fn prop_tuple_order(a: (i32, String), b: (i32, String)) {
            prop_assert_eq!(a.cmp(&b), enc(&a).cmp(&enc(&b)));
        }

        #[test]
        fn prop_option_order(a: Option<u64>, b: Option<u64>) {
            prop_assert_eq!(a.cmp(&b), enc(&a).cmp(&enc(&b)));
        }

        #[test]
        fn prop_u64_roundtrip(a: u64) {
            prop_assert_eq!(<OrderedCodec<u64> as KeyCodec<u64>>::decode(&enc(&a)).unwrap(), a);
        }

        #[test]
        fn prop_string_roundtrip(a: String) {
            prop_assert_eq!(<OrderedCodec<String> as KeyCodec<String>>::decode(&enc(&a)).unwrap(), a);
        }

        #[test]
        fn prop_tuple_roundtrip(a: (i32, String, Option<u8>)) {
            let bytes = enc(&a);
            prop_assert_eq!(
                <OrderedCodec<(i32, String, Option<u8>)> as KeyCodec<(i32, String, Option<u8>)>>::decode(&bytes).unwrap(),
                a
            );
        }
    }
}
