Using RudderStack eventstream data, this dbt project creates several metrics tables that can be plugged into various visualisation tools such as Tableau or Looker. 

## Setup and Dependencies
* RudderStack event-stream sdk is being used, which creates the following tables in your warehouse:
    * tracks
    * pages
    * identifies
* Run `dbt deps` which installs the required dependencies


### Setting up

All the changes can be done from the `dbt_project.yml` file. The key variables that require to be changed are:

```

vars:


```

Along with the above variables, the table names (variables that start with `tbl_` prefix) may need to be changed depending on the schema, both in the dbt_project.yml file and in schema.yml file if they deviate from the shopify spec. 



## Configuration

### Customisation

## Output:
[TODO]

## Metrics created


