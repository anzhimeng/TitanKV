#include <iostream>
#include <cstring>
#include <vector>

int main() {
    // 0x9B vs 0x37
    unsigned char u1 = 0x9B;
    unsigned char u2 = 0x37;
    
    char s1 = (char)0x9B;
    char s2 = (char)0x37;
    
    int r_unsigned = memcmp(&u1, &u2, 1);
    int r_signed = memcmp(&s1, &s2, 1);
    
    std::cout << "0x9B vs 0x37:" << std::endl;
    std::cout << "Unsigned memcmp: " << r_unsigned << std::endl;
    std::cout << "Signed chars memcmp: " << r_signed << std::endl;
    
    if (r_unsigned > 0) std::cout << "0x9B > 0x37 (Unsigned)" << std::endl;
    else std::cout << "0x9B < 0x37 (Unsigned)" << std::endl;

    if (r_signed > 0) std::cout << "0x9B > 0x37 (Signed?)" << std::endl;
    else std::cout << "0x9B < 0x37 (Signed?)" << std::endl;

    return 0;
}
