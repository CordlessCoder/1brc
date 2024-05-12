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

const BUMP_CAP: usize = 1024 * 1024 * 8;
const SINGLE_BUMP_MAX: usize = 1024;

#[derive(Debug, Clone, Copy, Default)]
struct MeasurementRecord {
    count: usize,
    sum: i64,
    min: i64,
    max: i64,
}

struct AtomicBumpAlloc {
    len: AtomicUsize,
    ptr: AtomicPtr<u8>,
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

impl AtomicBumpAlloc {
    pub const fn new() -> Self {
        // This is fine as the offset being at bump capacity will cause the first .alloc call in
        // any thread to trigger a reallocation
        AtomicBumpAlloc {
            len: AtomicUsize::new(BUMP_CAP - 1),
            ptr: AtomicPtr::new(std::ptr::null_mut()),
        }
    }
    pub fn alloc(&self, len: usize) -> &'static mut [MaybeUninit<u8>] {
        if len == 0 {
            return &mut [];
        }

        if len > SINGLE_BUMP_MAX {
            unsafe {
                let ptr = alloc(Layout::array::<u8>(len).unwrap());
                return std::slice::from_raw_parts_mut(ptr as *mut MaybeUninit<u8>, len);
            }
        };

        loop {
            std::sync::atomic::fence(Ordering::Acquire);
            let mut offset = self.len.fetch_add(len, Ordering::AcqRel);
            let mut ptr = self.ptr.load(Ordering::Acquire);
            std::sync::atomic::fence(Ordering::Release);

            // The idea is that only one thread will ever see the end cross over the BUMP_CAP
            // threshold, and that thread will be the one responsible for the reallocation
            let mut end = offset + len;

            if offset >= BUMP_CAP {
                // Reallocation in progress by another thread, keep polling until it's done
                continue;
            }

            if end >= BUMP_CAP {
                // We need to reallocate
                ptr = new_chunk();
                offset = 0;
                end = len;

                // Our allocation will immediately take the first len bytes
                self.ptr.store(ptr, Ordering::Release);
                self.len.store(len, Ordering::Release);
                std::sync::atomic::fence(Ordering::Release);
            }

            return unsafe {
                std::slice::from_raw_parts_mut(ptr.add(offset) as *mut MaybeUninit<u8>, len)
            };
        }
    }
    pub fn alloc_slice(&self, slice: &[u8]) -> &'static mut [u8] {
        unsafe {
            let bytes = self.alloc(slice.len());
            bytes
                .as_mut_ptr()
                .copy_from_nonoverlapping(slice.as_ptr() as *const MaybeUninit<u8>, slice.len());
            std::mem::transmute(bytes)
        }
    }
}

fn work(
    data: &[u8],
    per_thread: usize,
    thread: usize,
) -> Option<HashMap<&'static [u8], MeasurementRecord>> {
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
                    BUMP.alloc_slice(station),
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

        let station = &data[..semicolon];
        data = &data[semicolon + 1..];

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

static BUMP: AtomicBumpAlloc = AtomicBumpAlloc::new();

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
        let mean = sum / count as i64;
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
