// Platform accounting is adapted from Captain Miao; see LICENSE-CAPTAIN-MIAO.
#![allow(unsafe_code)]

use std::{
    path::Path,
    time::{Duration, Instant},
};

const MAX_CPU_WINDOW: Duration = Duration::from_secs(60);

#[derive(Clone, Debug, Default, PartialEq)]
pub struct HostSample {
    pub cpu_percent: Option<f64>,
    pub memory_used_bytes: Option<u64>,
    pub memory_total_bytes: Option<u64>,
    pub disk_available_bytes: Option<u64>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct CpuCounters {
    busy: u64,
    total: u64,
}

#[derive(Default)]
pub struct HostSampler {
    previous_cpu: Option<(Instant, CpuCounters)>,
}

impl HostSampler {
    pub fn sample(&mut self, home: &Path) -> HostSample {
        let now = Instant::now();
        let cpu_percent = self.observe_cpu(now, platform::cpu());
        let (memory_used_bytes, memory_total_bytes) = platform::memory();
        HostSample {
            cpu_percent,
            memory_used_bytes,
            memory_total_bytes,
            disk_available_bytes: available_disk(home),
        }
    }

    fn observe_cpu(&mut self, now: Instant, counters: Option<CpuCounters>) -> Option<f64> {
        let current = counters?;
        let previous = self.previous_cpu.replace((now, current));
        let (at, previous) = previous?;
        if now.checked_duration_since(at)? > MAX_CPU_WINDOW {
            return None;
        }
        let total = current.total.checked_sub(previous.total)?;
        let busy = current.busy.checked_sub(previous.busy)?;
        if total == 0 || busy > total {
            return None;
        }
        Some(busy as f64 / total as f64 * 100.0)
    }
}

fn available_disk(path: &Path) -> Option<u64> {
    use std::os::unix::ffi::OsStrExt;
    let path = std::ffi::CString::new(path.as_os_str().as_bytes()).ok()?;
    let mut output = std::mem::MaybeUninit::<libc::statvfs>::uninit();
    // The call writes a statvfs only on success; path is NUL-terminated.
    if unsafe { libc::statvfs(path.as_ptr(), output.as_mut_ptr()) } != 0 {
        return None;
    }
    let output = unsafe { output.assume_init() };
    #[allow(clippy::unnecessary_cast)]
    disk_bytes(output.f_bavail as u64, output.f_frsize as u64)
}

fn disk_bytes(available: u64, block_size: u64) -> Option<u64> {
    (block_size > 0)
        .then(|| available.checked_mul(block_size))
        .flatten()
}

#[cfg(target_os = "linux")]
mod platform {
    use super::{CpuCounters, parse_linux_cpu, parse_linux_memory};
    use std::io::Read;

    fn proc_text(path: &str) -> Option<String> {
        let mut text = String::new();
        std::fs::File::open(path)
            .ok()?
            .take(64 * 1024)
            .read_to_string(&mut text)
            .ok()?;
        Some(text)
    }

    pub(super) fn cpu() -> Option<CpuCounters> {
        parse_linux_cpu(&proc_text("/proc/stat")?)
    }

    pub(super) fn memory() -> (Option<u64>, Option<u64>) {
        proc_text("/proc/meminfo")
            .map(|text| parse_linux_memory(&text))
            .unwrap_or_default()
    }
}

#[cfg(any(target_os = "linux", test))]
fn parse_linux_cpu(text: &str) -> Option<CpuCounters> {
    let mut fields = text.lines().next()?.split_whitespace();
    if fields.next()? != "cpu" {
        return None;
    }
    // Guest counters are already included in user/nice. I/O wait is idle.
    let ticks = fields
        .take(8)
        .map(str::parse::<u64>)
        .collect::<Result<Vec<_>, _>>()
        .ok()?;
    if ticks.len() < 4 {
        return None;
    }
    let total = ticks.iter().try_fold(0_u64, |sum, n| sum.checked_add(*n))?;
    let idle = ticks[3].checked_add(ticks.get(4).copied().unwrap_or(0))?;
    Some(CpuCounters {
        busy: total.checked_sub(idle)?,
        total,
    })
}

#[cfg(any(target_os = "linux", test))]
fn parse_linux_memory(text: &str) -> (Option<u64>, Option<u64>) {
    let (mut total, mut available) = (None, None);
    for line in text.lines() {
        let Some((name, value)) = line.split_once(':') else {
            continue;
        };
        let target = match name {
            "MemTotal" => &mut total,
            "MemAvailable" => &mut available,
            _ => continue,
        };
        let mut fields = value.split_whitespace();
        *target = fields
            .next()
            .and_then(|value| value.parse::<u64>().ok())
            .filter(|_| fields.next() == Some("kB"))
            .and_then(|value| value.checked_mul(1024));
    }
    let total = total.filter(|n| *n > 0);
    let used = total
        .zip(available)
        .and_then(|(total, free)| total.checked_sub(free));
    (used, total)
}

#[cfg(target_os = "macos")]
mod platform {
    use super::CpuCounters;
    use std::sync::OnceLock;

    fn host() -> libc::mach_port_t {
        // Keep one send right for this process, rather than acquiring one per probe.
        static PORT: OnceLock<libc::mach_port_t> = OnceLock::new();
        *PORT.get_or_init(|| unsafe {
            #[allow(deprecated)]
            libc::mach_host_self()
        })
    }

    pub(super) fn cpu() -> Option<CpuCounters> {
        let mut output = std::mem::MaybeUninit::<libc::host_cpu_load_info>::zeroed();
        let mut count = libc::HOST_CPU_LOAD_INFO_COUNT;
        // The flavor and count describe exactly the supplied host_cpu_load_info.
        let result = unsafe {
            libc::host_statistics(
                host(),
                libc::HOST_CPU_LOAD_INFO,
                output.as_mut_ptr().cast(),
                &raw mut count,
            )
        };
        if result != 0 || count != libc::HOST_CPU_LOAD_INFO_COUNT {
            return None;
        }
        let output = unsafe { output.assume_init() };
        let total = output.cpu_ticks.iter().map(|n| u64::from(*n)).sum::<u64>();
        Some(CpuCounters {
            busy: total.checked_sub(u64::from(output.cpu_ticks[libc::CPU_STATE_IDLE as usize]))?,
            total,
        })
    }

    pub(super) fn memory() -> (Option<u64>, Option<u64>) {
        let total = memory_size();
        let mut output = std::mem::MaybeUninit::<libc::vm_statistics64>::zeroed();
        let mut count = libc::HOST_VM_INFO64_COUNT;
        // The flavor and count describe exactly the supplied vm_statistics64.
        let result = unsafe {
            libc::host_statistics64(
                host(),
                libc::HOST_VM_INFO64,
                output.as_mut_ptr().cast(),
                &raw mut count,
            )
        };
        if result != 0 || count != libc::HOST_VM_INFO64_COUNT {
            return (None, total);
        }
        let output = unsafe { output.assume_init() };
        let page_size = unsafe { libc::sysconf(libc::_SC_PAGESIZE) };
        let used = u64::try_from(page_size)
            .ok()
            .filter(|size| *size > 0)
            .and_then(|size| {
                // Active, wired and compressor pages are occupied; inactive pages
                // can be reclaimed and are excluded from memory used.
                (u64::from(output.active_count)
                    + u64::from(output.wire_count)
                    + u64::from(output.compressor_page_count))
                .checked_mul(size)
            })
            .filter(|used| total.is_none_or(|total| *used <= total));
        (used, total)
    }

    fn memory_size() -> Option<u64> {
        let mut value = 0_u64;
        let mut length = size_of::<u64>();
        // The output buffer holds one u64. No new value is supplied.
        let result = unsafe {
            libc::sysctlbyname(
                c"hw.memsize".as_ptr(),
                (&raw mut value).cast(),
                &raw mut length,
                std::ptr::null_mut(),
                0,
            )
        };
        (result == 0 && length == size_of::<u64>() && value > 0).then_some(value)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cpu_counts_iowait_as_idle_and_does_not_double_count_guests() {
        assert_eq!(
            parse_linux_cpu("cpu 10 2 3 80 4 1 2 3 8 1\ncpu0 1 1 1 1"),
            Some(CpuCounters {
                busy: 21,
                total: 105
            })
        );
        assert!(parse_linux_cpu("cpu 10 20 30").is_none());
        assert!(parse_linux_cpu("cpu 1 nope 3 4").is_none());
        assert!(parse_linux_cpu("cpu 18446744073709551615 1 0 0").is_none());
    }

    #[test]
    fn cpu_first_missing_stale_and_reset_samples_are_not_zeroes() {
        let mut sampler = HostSampler::default();
        let now = Instant::now();
        let a = CpuCounters {
            busy: 100,
            total: 1000,
        };
        let b = CpuCounters {
            busy: 125,
            total: 1100,
        };
        assert_eq!(sampler.observe_cpu(now, Some(a)), None);
        assert_eq!(
            sampler.observe_cpu(now + Duration::from_secs(1), None),
            None
        );
        assert_eq!(
            sampler.observe_cpu(now + Duration::from_secs(2), Some(b)),
            Some(25.0)
        );
        assert_eq!(
            sampler.observe_cpu(now + Duration::from_secs(3), Some(b)),
            None
        );
        assert_eq!(
            sampler.observe_cpu(now + Duration::from_secs(4), Some(a)),
            None
        );
        assert_eq!(
            sampler.observe_cpu(now + Duration::from_secs(65), Some(b)),
            None
        );
        assert_eq!(
            sampler.observe_cpu(
                now + Duration::from_secs(66),
                Some(CpuCounters {
                    busy: 125,
                    total: 1200
                })
            ),
            Some(0.0)
        );
        assert_eq!(
            sampler.observe_cpu(
                now + Duration::from_secs(67),
                Some(CpuCounters {
                    busy: 300,
                    total: 1250
                })
            ),
            None
        );
    }

    #[test]
    fn memory_uses_available_memory_and_rejects_invalid_units_and_overflow() {
        assert_eq!(
            parse_linux_memory("MemTotal: 100 kB\nMemFree: 2 kB\nMemAvailable: 40 kB"),
            (Some(60 * 1024), Some(100 * 1024))
        );
        assert_eq!(
            parse_linux_memory("MemTotal: 100 kB\nMemFree: 2 kB"),
            (None, Some(100 * 1024))
        );
        assert_eq!(
            parse_linux_memory("MemTotal: 100 kB\nMemAvailable: 101 kB"),
            (None, Some(100 * 1024))
        );
        assert_eq!(
            parse_linux_memory("MemTotal: 100 B\nMemAvailable: 1 kB"),
            (None, None)
        );
        assert_eq!(
            parse_linux_memory("MemTotal: 18446744073709551615 kB"),
            (None, None)
        );
        assert_eq!(parse_linux_memory(""), (None, None));
    }

    #[test]
    fn disk_availability_is_user_available_space_and_can_be_zero() {
        assert_eq!(disk_bytes(17, 4096), Some(17 * 4096));
        assert_eq!(disk_bytes(0, 4096), Some(0));
        assert_eq!(disk_bytes(1, 0), None);
        assert_eq!(disk_bytes(u64::MAX, 4096), None);
        assert_eq!(available_disk(Path::new("/a\0b")), None);
    }
}
