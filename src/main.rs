#![allow(unused)]
use std::{
    alloc::{alloc, Layout},
    error::Error,
    fs::File,
    hint::black_box,
    io::{stdout, BufWriter, Write},
    mem::MaybeUninit,
    ops::RangeBounds,
    ptr::NonNull,
    sync::atomic::{AtomicPtr, AtomicUsize, Ordering},
    time::Instant,
};

use hashbrown::HashMap;
use memmap2::Mmap;

const BUMP_CAP: usize = 1024 * 1024 * 4;
const _: () = assert!(BUMP_CAP > 1024);
// const SINGLE_BUMP_MAX: usize = 1024;

#[derive(Debug, Clone, Copy, Default)]
struct MeasurementRecord {
    count: usize,
    sum: i64,
    min: i64,
    max: i64,
}

impl Default for BumpAlloc {
    fn default() -> Self {
        Self {
            len: 0,
            ptr: new_chunk(),
        }
    }
}
struct BumpAlloc {
    len: usize,
    ptr: *mut u8,
}

impl BumpAlloc {
    #[inline]
    pub fn new_nonlazy() -> Self {
        Self::default()
    }
    #[inline]
    pub const fn new() -> Self {
        Self {
            len: BUMP_CAP,
            ptr: std::ptr::null_mut(),
        }
    }
    #[inline]
    pub fn alloc(&mut self, len: usize) -> &'static mut [MaybeUninit<u8>] {
        unsafe {
            // // SAFETY: Technically required for the case where the length wouldn't fit in the
            // // backing buffer, but we know all stations will be under than 100 bytes
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

fn work(
    data: &[u8],
    per_thread: usize,
    thread: usize,
) -> Option<HashMap<&'static [u8], MeasurementRecord>> {
    let mut bump = BumpAlloc::new_nonlazy();
    let mut start = per_thread * thread;
    let end = start + per_thread;

    if start != 0 {
        let (first_newline, _) = data
            .iter()
            .enumerate()
            .skip(start)
            .take(per_thread)
            .find(|(_, &b)| b == b'\n')?;
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

    let mut map: HashMap<&[u8], MeasurementRecord> = HashMap::new();
    let mut handle_entry = |station: &[u8], value: i64| {
        // let station: &'static [u8] =
        // _ = unsafe { dbg!(std::str::from_utf8_unchecked(station), value) };
        let (_, record) = map
            .raw_entry_mut()
            .from_key(station)
            .and_modify(|_, rec| {
                rec.count += 1;
                rec.sum += value;
                rec.min = rec.min.min(value);
                rec.max = rec.max.max(value);
            })
            .or_insert_with(|| {
                (
                    bump.alloc_slice(station),
                    MeasurementRecord {
                        count: 1,
                        sum: value,
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
            1 => before_dot[0].wrapping_sub(b'0') as i64 * 10 + after_dot.wrapping_sub(b'0') as i64,
            2 => {
                if before_dot[0] == b'-' {
                    -(before_dot[1].wrapping_sub(b'0') as i64) * 10
                        - after_dot.wrapping_sub(b'0') as i64
                } else {
                    (before_dot[0].wrapping_sub(b'0') as i64 * 100)
                        + (before_dot[1].wrapping_sub(b'0') as i64 * 10)
                        + after_dot.wrapping_sub(b'0') as i64
                }
            }
            3 => {
                -(before_dot[1].wrapping_sub(b'0') as i64 * 100
                    + before_dot[2].wrapping_sub(b'0') as i64 * 10
                    + after_dot.wrapping_sub(b'0') as i64)
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
    Some(map)
}

fn main() -> Result<(), Box<dyn Error>> {
    let threads = std::thread::available_parallelism().unwrap().get();

    let path = std::env::args().nth(1);
    let path = path.as_deref().unwrap_or("measurements.txt");

    let file = File::open(path).unwrap();
    let data = unsafe { Mmap::map(&file).unwrap() };
    let data = &data[..];

    let per_thread = (data.len() + threads - 1) / threads;

    let map = std::thread::scope(|s| {
        let mut handles = Vec::with_capacity(threads);
        for thread in 1..threads {
            let thread = s.spawn(move || work(data, per_thread, thread));
            handles.push(thread);
        }
        let mut map = work(data, per_thread, 0);
        handles.into_iter().for_each(|h| {
            let res = h.join().unwrap();
            if let Some(map) = map.as_mut() {
                if let Some(res) = res {
                    res.into_iter().for_each(|(station, data)| {
                        let rec = map
                            .entry(station)
                            .and_modify(|rec| {
                                rec.count += data.count;
                                rec.sum += data.sum;
                                rec.max = rec.max.max(data.max);
                                rec.min = rec.min.min(data.min);
                            })
                            .or_insert(data);
                    })
                }
            } else {
                map = res;
            }
        });
        map.unwrap_or_default()
    });
    let mut stations: Vec<_> = map.into_iter().collect();
    stations.sort_unstable_by_key(|&(s, _)| s);
    let mut output = BufWriter::with_capacity(1024 * 1024, stdout().lock());
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
        let min_a = min / 10;
        let min_b = (min.unsigned_abs() % 10) as u8;
        let mean_a = mean / 10;
        let mean_b = (mean.unsigned_abs() % 10) as u8;
        let max_a = max / 10;
        let max_b = (max.unsigned_abs() % 10) as u8;
        writeln!(output, ";{min_a}.{min_b};{mean_a}.{mean_b};{max_a}.{max_b}")?;
    }
    Ok(())
}
