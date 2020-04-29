------------------------------------------------------------- 1. cpu -------------------------------------------------------------
总核数 = 物理CPU个数 X 每颗物理CPU的核数  
总逻辑CPU数 = 物理CPU个数 X 每颗物理CPU的核数 X 超线程

1.1 查看物理CPU个数
 cat /proc/cpuinfo | grep "physical id"| sort| uniq| wc -l
 
 1.2  查看每个物理CPU中core的个数(即核数）
  cat /proc/cpuinfo | grep "cpu cores"| uniq
  
 1.3  查看逻辑CPU的个数
   cat /proc/cpuinfo | grep "processor"| wc -l
   
 1.4  查看CPU信息（型号）
   cat /proc/cpuinfo | grep name | cut -f2 -d: | uniq -c
 
 1.5 查看各个物理CPU上面封装的逻辑处理器（即超线程后的CPU）个数
   cat /proc/cpuinfo | grep siblings
   
 1.6 查看CPU明细
    Architecture:          x86_64       #架构
    CPU op-mode(s):        32-bit, 64-bit
    Byte Order:            Little Endian
    CPU(s):                36                       #逻辑cpu颗数是36
    On-line CPU(s) list:   0-35
    Thread(s) per core:    1                       #每个核心线程数是1
    Core(s) per socket:    1                       #每个cpu插槽核数/每颗物理cpu核数是2
    Socket(s):             36
    NUMA node(s):          2
    Vendor ID:             GenuineIntel
    CPU family:            6
    Model:                 63
    Model name:            Intel(R) Xeon(R) CPU E5-2678 v3 @ 2.50GHz
    Stepping:              2
    CPU MHz:               2499.998
    BogoMIPS:              4999.99
    Hypervisor vendor:     VMware
    Virtualization type:   full
    L1d cache:             32K                     #一级缓存32K
    L1i cache:             32K                     #一级缓存32K
    L2 cache:              256K                    #二级缓存256k
    L3 cache:              30720K                  #三级缓存3072k
    NUMA node0 CPU(s):     0-17
    NUMA node1 CPU(s):     18-35


------------------------------------------------------------- 2. 内存 -------------------------------------------------------------
