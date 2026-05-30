#pragma once

#include <vector>
#include <string>
#include "titankv/slice.h"
#include "titankv/status.h"

namespace titankv {

enum class CoprocessorType : uint8_t {
    kCount = 0,
    kSum = 1,
    // Add more types here
};

enum class FilterOperator : uint8_t {
    kEqual = 0,
    kNotEqual = 1,
    kGreater = 2,
    kLess = 3,
    kGreaterOrEqual = 4,
    kLessOrEqual = 5,
};

struct CoprocessorRequest {
    CoprocessorType type;
    std::string start_key;
    std::string end_key;
    uint64_t start_ts;
    // For filters
    std::string filter_value;
    FilterOperator filter_operator = FilterOperator::kEqual;
};

struct CoprocessorResponse {
    uint64_t count = 0;
    int64_t sum = 0;
    std::string error_msg;
};

class CoprocessorHost {
public:
    virtual ~CoprocessorHost() = default;
    virtual Status Execute(const CoprocessorRequest& req, CoprocessorResponse* resp) = 0;
};

} // namespace titankv
