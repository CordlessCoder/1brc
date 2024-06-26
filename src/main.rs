use std::{
    alloc::{alloc, Layout},
    error::Error,
    fs::File,
    io::{stdout, BufWriter, Write},
    mem::MaybeUninit,
    sync::atomic::AtomicUsize,
};

use hashbrown::HashMap;
use memmap2::Mmap;

const BUMP_CAP: usize = 1024 * 1024;
const _: () = assert!(BUMP_CAP > 1024);
// const SINGLE_BUMP_MAX: usize = 1024;

const WORK_CHUNK: usize = 1024 * 1024 * 2;

type Map<K, V> = HashMap<K, V>;

#[derive(Debug, Clone, Copy, Default)]
struct MeasurementRecord {
    count: usize,
    sum: i64,
    min: i16,
    max: i16,
}

struct BumpAlloc {
    len: usize,
    ptr: *mut u8,
}

impl BumpAlloc {
    #[inline]
    pub fn new() -> Self {
        Self {
            len: 0,
            ptr: new_chunk(),
        }
    }
    #[inline]
    pub fn alloc(&mut self, len: usize) -> &'static mut [MaybeUninit<u8>] {
        unsafe {
            // // SAFETY: Technically required for the case where the length wouldn't fit in the
            // // backing buffer, but we know all stations will be under 100 bytes
            //
            // if len > SINGLE_BUMP_MAX {
            //     let ptr = alloc(Layout::array::<u8>(len).unwrap());
            //     return std::slice::from_raw_parts_mut(ptr as *mut MaybeUninit<u8>, len);
            // }
            if (self.len + len) > BUMP_CAP {
                self.ptr = new_chunk();
                self.len = 0;
            }
            let ptr = self.ptr.add(self.len);
            self.len += len;
            std::slice::from_raw_parts_mut(ptr as *mut MaybeUninit<u8>, len)
        }
    }
    pub fn alloc_slice(&mut self, slice: &[u8]) -> &'static mut [u8] {
        unsafe {
            let bytes = self.alloc(slice.len());
            bytes
                .as_mut_ptr()
                .copy_from_nonoverlapping(slice.as_ptr() as *const MaybeUninit<u8>, slice.len());
            std::mem::transmute(bytes)
        }
    }
}

fn new_chunk() -> *mut u8 {
    unsafe {
        let ptr = alloc(Layout::array::<u8>(BUMP_CAP).unwrap_unchecked());
        if ptr.is_null() {
            std::alloc::handle_alloc_error(Layout::array::<u8>(BUMP_CAP).unwrap_unchecked())
        };
        ptr
    }
}

fn work(data: &[u8], cursor: &AtomicUsize) -> Map<&'static [u8], MeasurementRecord> {
    #[inline(always)]
    fn process_chunk(
        data: &[u8],
        mut start: usize,
        end: usize,
        map: &mut Map<&'static [u8], MeasurementRecord>,
    ) {
        let mut bump = BumpAlloc::new();
        if start != 0 {
            let Some((first_newline, _)) = data
                .iter()
                .enumerate()
                .take(end)
                .skip(start)
                .find(|(_, &b)| b == b'\n')
            else {
                return;
            };
            // the +1 is necessary to skip the first newline
            start = first_newline + 1;
        }
        let end = data
            .iter()
            .enumerate()
            .skip(end)
            .find(|(_, &b)| b == b'\n')
            .map(|(end, _)| end)
            .unwrap_or(data.len());

        let mut data = &data[start..end];

        // _ = unsafe { dbg!(thread, std::str::from_utf8_unchecked(data)) };

        let mut handle_entry = |station: &[u8], value: i16| {
            // let station: &'static [u8] =
            // _ = unsafe { dbg!(std::str::from_utf8_unchecked(station), value) };
            map.raw_entry_mut()
                .from_key(station)
                .and_modify(|_, rec| {
                    rec.count += 1;
                    rec.sum += value as i64;
                    rec.min = rec.min.min(value);
                    rec.max = rec.max.max(value);
                })
                .or_insert_with(|| {
                    (
                        bump.alloc_slice(station),
                        MeasurementRecord {
                            count: 1,
                            sum: value as i64,
                            min: value,
                            max: value,
                        },
                    )
                });
        };
        while !data.is_empty() {
            // Hamburg;12.0...
            let semicolon = data.iter().position(|&b| b == b';');
            #[cfg(debug_assertions)]
            let semicolon = semicolon.unwrap();
            #[cfg(not(debug_assertions))]
            let semicolon = unsafe { semicolon.unwrap_unchecked() };

            #[cfg(debug_assertions)]
            let station = &data[..semicolon];
            #[cfg(not(debug_assertions))]
            let station = unsafe { data.get_unchecked(..semicolon) };
            #[cfg(debug_assertions)]
            let rem = &data[semicolon + 1..];
            #[cfg(not(debug_assertions))]
            let rem = unsafe { data.get_unchecked(semicolon + 1..) };
            data = rem;

            let dot = data.iter().position(|&b| b == b'.');
            #[cfg(debug_assertions)]
            let dot = dot.unwrap();
            #[cfg(not(debug_assertions))]
            let dot = unsafe { dot.unwrap_unchecked() };

            #[cfg(debug_assertions)]
            let before_dot = &data[..dot];
            #[cfg(not(debug_assertions))]
            let before_dot = unsafe { data.get_unchecked(..dot) };
            #[cfg(debug_assertions)]
            let after_dot = data[dot + 1];
            #[cfg(not(debug_assertions))]
            let after_dot = unsafe { data.get_unchecked(dot + 1) };

            let value = match before_dot.len() {
                1 => {
                    before_dot[0].wrapping_sub(b'0') as i16 * 10
                        + after_dot.wrapping_sub(b'0') as i16
                }
                2 => {
                    if before_dot[0] == b'-' {
                        -(before_dot[1].wrapping_sub(b'0') as i16) * 10
                            - after_dot.wrapping_sub(b'0') as i16
                    } else {
                        (before_dot[0].wrapping_sub(b'0') as i16 * 100)
                            + (before_dot[1].wrapping_sub(b'0') as i16 * 10)
                            + after_dot.wrapping_sub(b'0') as i16
                    }
                }
                3 => {
                    -(before_dot[1].wrapping_sub(b'0') as i16 * 100
                        + before_dot[2].wrapping_sub(b'0') as i16 * 10
                        + after_dot.wrapping_sub(b'0') as i16)
                }
                _ => {
                    #[cfg(debug_assertions)]
                    unreachable!();
                    #[cfg(not(debug_assertions))]
                    unsafe {
                        std::hint::unreachable_unchecked()
                    };
                }
            };

            handle_entry(station, value);

            let Some(remainder) = data.get(dot + 3..) else {
                break;
            };
            data = remainder;
        }
    }

    let mut map: Map<&[u8], MeasurementRecord> = Map::with_capacity(1024 * 8);
    loop {
        let offset = cursor.fetch_add(WORK_CHUNK, std::sync::atomic::Ordering::Release);
        let end = offset + WORK_CHUNK;
        let end = end.min(data.len());
        if offset >= data.len() {
            break;
        }
        process_chunk(data, offset, end, &mut map)
    }
    map
}

fn main() -> Result<(), Box<dyn Error>> {
    let threads = std::thread::available_parallelism().unwrap().get();

    // let path = std::env::args().nth(1);
    // let path = path.as_deref().unwrap_or("measurements.txt");
    let path = "measurements.txt";

    let file = File::open(path).unwrap();
    let data = unsafe { Mmap::map(&file).unwrap() };
    let data = &data[..];

    let cursor = AtomicUsize::new(0);

    let map = std::thread::scope(|s| {
        let mut handles = Vec::with_capacity(threads);
        let cursor = &cursor;
        for _ in 1..threads {
            let thread = s.spawn(move || work(data, cursor));
            handles.push(thread);
        }
        let mut map = work(data, cursor);
        handles.into_iter().for_each(|h| {
            let res = h.join().unwrap();
            res.into_iter().for_each(|(station, data)| {
                map.entry(station)
                    .and_modify(|rec| {
                        rec.count += data.count;
                        rec.sum += data.sum;
                        rec.max = rec.max.max(data.max);
                        rec.min = rec.min.min(data.min);
                    })
                    .or_insert(data);
            })
        });
        map
    });
    let mut stations: Vec<_> = map.into_iter().collect();
    stations.sort_unstable_by_key(|&(s, _)| s);
    let mut output = BufWriter::with_capacity(1024 * 512, stdout().lock());
    for (
        station,
        MeasurementRecord {
            count,
            sum,
            min,
            max,
        },
    ) in stations
    {
        let mean = if sum ^ (count as i64) >= 0 {
            (sum + ((count as i64) / 2)) / (count as i64)
        } else {
            (sum - ((count as i64) / 2)) / (count as i64)
        };
        output.write_all(station)?;
        fn format_fixed(buf: &mut [u8; 5], n: i64) -> &[u8] {
            let todigit = |n| n as u8 + b'0';
            match n {
                n @ 100..=999 => {
                    buf[0] = todigit(n / 100);
                    buf[1] = todigit(n / 10 % 10);
                    buf[2] = b'.';
                    buf[3] = todigit(n % 10);
                    &buf[0..4]
                }
                n @ 0..=99 => {
                    buf[0] = todigit(n / 10 % 10);
                    buf[1] = b'.';
                    buf[2] = todigit(n % 10);
                    &buf[0..3]
                }
                n @ -99..=-1 => {
                    let n = -n;
                    buf[0] = b'-';
                    buf[1] = todigit(n / 10 % 10);
                    buf[2] = b'.';
                    buf[3] = todigit(n % 10);
                    &buf[0..4]
                }
                n @ -999..=-100 => {
                    let n = -n;
                    buf[0] = b'-';
                    buf[1] = todigit(n / 100 % 10);
                    buf[2] = todigit(n / 10 % 10);
                    buf[3] = b'.';
                    buf[4] = todigit(n % 10);
                    &buf[0..5]
                }
                i64::MIN..=-1000 | 1000..=i64::MAX => {
                    #[cfg(debug_assertions)]
                    unreachable!("All fixed-precision numbers should be in the range -999..=999");
                    #[cfg(not(debug_assertions))]
                    unsafe {
                        std::hint::unreachable_unchecked()
                    };
                }
            }
        }
        let mut buf = [0; 5];
        _ = output.write(b";")?;

        let min = format_fixed(&mut buf, min as i64);
        output.write_all(min)?;
        _ = output.write(b";")?;

        let mean = format_fixed(&mut buf, mean);
        output.write_all(mean)?;
        _ = output.write(b";")?;

        let max = format_fixed(&mut buf, max as i64);
        output.write_all(max)?;
        _ = output.write(b"\n")?;
    }
    Ok(())
}
