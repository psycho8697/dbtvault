{{ dbtvault.stage(source_model=var('source_model', none),
                  hashed_columns=var('hashed_columns'), 
                  derived_columns=var('derived_columns')) }}