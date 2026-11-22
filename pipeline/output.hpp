#pragma once
#include <arrow/api.h>
#include <arrow/io/api.h>
#include <parquet/arrow/writer.h>
#include <parquet/properties.h>

inline void checked(const arrow::Status& status) {
    if (!status.ok()) throw std::runtime_error(status.ToString());
}
template<class T> T checked(arrow::Result<T> value) {
    if (!value.ok()) throw std::runtime_error(value.status().ToString());
    return std::move(value).ValueOrDie();
}

inline void parquet_table(const fs::path& path, std::vector<std::string> names,
                          std::vector<std::shared_ptr<arrow::Array>> columns) {
    std::vector<std::shared_ptr<arrow::Field>> fields;
    for (size_t i=0; i<columns.size(); ++i) fields.push_back(arrow::field(names[i],columns[i]->type()));
    auto table=arrow::Table::Make(arrow::schema(fields),columns);
    auto file=checked(arrow::io::FileOutputStream::Open(path.string()));
    parquet::WriterProperties::Builder props;
    props.compression(parquet::Compression::SNAPPY);
    checked(parquet::arrow::WriteTable(*table,arrow::default_memory_pool(),file,65536,props.build()));
    checked(file->Close());
}

inline void write_users(const fs::path& path, const std::vector<UserRow>& rows,
                        const Dictionary& people, const Dictionary& categories) {
    arrow::StringBuilder user, category;
    arrow::Date32Builder day;
    arrow::UInt32Builder sessions, events;
    arrow::Int64Builder revenue;
    for (arrow::ArrayBuilder* b:std::array<arrow::ArrayBuilder*,6>{&user,&day,&sessions,&events,&revenue,&category})
        checked(b->Reserve(rows.size()));
    for (const auto& r:rows) {
        checked(user.Append(people.names[r.person]));
        day.UnsafeAppend(r.day);
        sessions.UnsafeAppend(r.sessions);
        events.UnsafeAppend(r.events);
        revenue.UnsafeAppend(r.revenue);
        if (r.category == NONE) checked(category.AppendNull());
        else checked(category.Append(categories.names[r.category]));
    }
    parquet_table(path,{"user_id","day","sessions","events","revenue_cents","top_category"},
                  {checked(user.Finish()),checked(day.Finish()),checked(sessions.Finish()),
                   checked(events.Finish()),checked(revenue.Finish()),checked(category.Finish())});
}

inline void write_campaigns(const fs::path& path, const Catalogs& catalogs, int64_t first_day,
                            size_t days, const std::vector<int64_t>& totals) {
    arrow::StringBuilder id,channel;
    arrow::Date32Builder day;
    std::array<arrow::Int64Builder,4> metrics;
    auto organic=catalogs.campaigns.names.size();
    for (size_t c=0; c<=organic; ++c) for(size_t d=0; d<days; ++d) {
        const auto* row=&totals[(c*days+d)*4];
        if (!row[0] && !row[3]) continue;
        checked(id.Append(c==organic ? Text("organic") : catalogs.campaigns.names[c]));
        checked(channel.Append(c==organic ? Text("organic") : catalogs.channels[c]));
        checked(day.Append(int32_t(first_day+d)));
        for (size_t k=0;k<4;++k) checked(metrics[k].Append(row[k]));
    }
    parquet_table(path,{"campaign_id","channel","day","sessions","purchasing_sessions",
                        "revenue_cents","assisted_sessions"},
                  {checked(id.Finish()),checked(channel.Finish()),checked(day.Finish()),
                   checked(metrics[0].Finish()),checked(metrics[1].Finish()),
                   checked(metrics[2].Finish()),checked(metrics[3].Finish())});
}
