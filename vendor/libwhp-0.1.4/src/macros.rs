// Copyright 2018 Cloudbase Solutions Srl
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may
// not use this file except in compliance with the License. You may obtain
// a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// License for the specific language governing permissions and limitations
// under the License.

// Based on https://github.com/retep998/winapi-rs/blob/becb23f4522bec535fdc3417e7c15b390851f2de/src/macros.rs#L308-L326
macro_rules! bitfield {
    ($base:ident $field:ident: $fieldtype:ty [
        $($thing:ident $set_thing:ident[$r:expr],)+
    ]) => {
        impl $base {$(
            #[inline]
            #[allow(non_snake_case)]
            pub fn $thing(&self) -> $fieldtype {
                let size = $crate::std::mem::size_of::<$fieldtype>() * 8;
                self.$field << (size - $r.end) >> (size - $r.end + $r.start)
            }
            #[inline]
            #[allow(non_snake_case)]
            pub fn $set_thing(&mut self, val: $fieldtype) {
                let mask = ((1 << ($r.end - $r.start)) - 1) << $r.start;
                self.$field &= !mask;
                self.$field |= (val << $r.start) & mask;
            }
        )+}

        impl std::fmt::Display for $base {
            fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
                writeln!(f, "{}:", stringify!($base)).unwrap();
                $(
                writeln!(f, "    {}: 0x{:x?}", stringify!($thing),
                         self.$thing()).unwrap();
                )+
                Ok(())
            }
        }
    }
}

// Assumes that WHV_UINT128 is used.
macro_rules! bitfield128 {
    ($base:ident $field:ident: $fieldtype:ty [
        $($thing:ident $set_thing:ident[$r:expr],)+
    ]) => {
        impl $base {$(
            #[inline]
            #[allow(non_snake_case)]
            pub fn $thing(&self) -> $fieldtype {
                // We assume that $r.start < $r.end and both are
                // either smaller or greater than 64.
                let diff = ($r.end + $r.start) & 0x3f;
                if $r.start < 64 {
                    self.$field.Low64 <<
                        match $r.end {
                            64 => 0,
                            _  => 64 - $r.end
                        } >> (64 - diff)
                }
                else {
                    self.$field.High64 <<
                        match $r.end {
                            128 => 0,
                            _ => 128 - $r.end
                        } >> (64 - diff)
                }
            }
            #[inline]
            #[allow(non_snake_case)]
            pub fn $set_thing(&mut self, val: $fieldtype) {
                let mask;
                let diff = $r.end - $r.start;
                if (diff >= 64) {
                    mask = u64::MAX << ($r.start % 64);
                }
                else {
                    mask = ((1 << diff) - 1) << ($r.start % 64);
                }
                if $r.start < 64 {
                    self.$field.Low64 &= !mask;
                    self.$field.Low64 |= val << ($r.start % 64) & mask;
                }
                else {
                    self.$field.High64 &= !mask;
                    self.$field.High64 |= val << ($r.start % 64) & mask;
                }
            }
        )+}

        impl std::fmt::Display for $base {
            fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
                writeln!(f, "{}:", stringify!($base)).unwrap();
                writeln!(f, "    {}: {:?}", stringify!($field),
                         self.$field).unwrap();
                $(
                writeln!(f, "    {}: 0x{:x?}", stringify!($thing),
                         self.$thing()).unwrap();
                )+
                Ok(())
            }
        }
    }
}

#[cfg(test)]
#[allow(dead_code)]
mod tests {
    use common::*;
    use win_hv_platform_defs::WHV_UINT128;

    #[test]
    fn test_bitfield() {
        #[derive(Debug, Copy, Clone, PartialEq, Eq, Hash, Default)]
        #[allow(non_snake_case)]
        #[repr(C)]
        struct TEST_STRUCT_U8 {
            pub other: UINT8,
            pub AsUINT8: UINT8,
        }

        bitfield!(TEST_STRUCT_U8 AsUINT8: UINT8[
            a set_a[0..2],
            b set_b[2..4],
            c set_c[4..8],
        ]);

        let mut obj = TEST_STRUCT_U8 {
            other: 5,
            AsUINT8: 4
        };

        assert_eq!(0, obj.a());
        assert_eq!(1, obj.b());
        assert_eq!(0, obj.c());

        obj.set_c(1 as u8);

        assert_eq!(1, obj.c());
        assert_eq!(20, obj.AsUINT8);
        assert_eq!(5, obj.other);
    }

    #[test]
    fn test_bitfield128() {
        #[derive(Debug, Copy, Clone, PartialEq, Eq, Hash, Default)]
        #[allow(non_snake_case)]
        #[repr(C)]
        struct TEST_STRUCT_U128 {
            pub AsUINT128: WHV_UINT128,
        }

        bitfield128!(TEST_STRUCT_U128 AsUINT128: UINT64[
            a set_a[0..32],
            b set_b[32..64],
            c set_c[64..96],
            d set_d[96..128],
        ]);

        let mut obj = TEST_STRUCT_U128 {
            AsUINT128: WHV_UINT128 {
                Low64: 1 << 32,
                High64: 1 << 32
            }
        };

        assert_eq!(0, obj.a());
        assert_eq!(1, obj.b());
        assert_eq!(0, obj.c());
        assert_eq!(1, obj.d());

        obj.set_c(2 as u64);

        assert_eq!(2, obj.c());
        assert_eq!(
            WHV_UINT128 {
                Low64: 1 << 32,
                High64: (1 << 32) | 2
            },
            obj.AsUINT128);

        obj.set_a(0x10111213);
        obj.set_b(0x14151617);
        obj.set_c(0x18191a1b);
        obj.set_d(0x1c1d1e1f);

        assert_eq!(0x10111213, obj.a());
        assert_eq!(0x14151617, obj.b());
        assert_eq!(0x18191a1b, obj.c());
        assert_eq!(0x1c1d1e1f, obj.d());
        assert_eq!(0x1415161710111213, obj.AsUINT128.Low64);
        assert_eq!(0x1c1d1e1f18191a1b, obj.AsUINT128.High64);
    }
}
