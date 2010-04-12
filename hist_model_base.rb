require 'digest/sha1'
require 'active_support'

class HistModelBase < ActiveRecord::Base
    # Declare this an abstract class, this is useful primarily because it
    # disables the automatic single table inheritance that ActiveRecord::Base
    # uses by default. The result is that classes that inherit this class
    # have their own tables in the db rather then all the child classes
    # sharing one table called HistModelBases.
    self.abstract_class = true
    
    # Central regexp implementations so that we aren't duplicating regexs
    # all over creation. Beyond that it allows use to use regexs within other
    # regexs as sub regexs.
    @@join_types_re = "INNER JOIN|CROSS JOIN|STRAIGHT_JOIN|NATURAL LEFT OUTER JOIN|NATURAL RIGHT OUTER JOIN|NATURAL LEFT JOIN|NATURAL RIGHT JOIN|LEFT OUTER JOIN|RIGHT OUTER JOIN|LEFT JOIN|RIGHT JOIN|JOIN|,"
    @@table_refs_delims_re = "WHERE|HAVING|GROUP BY|ORDER BY|LIMIT|PROCEDURE|INTO OUTFILE|INTO DUMPFILE|INTO|FOR UPDATE|LOCK IN SHARE MODE|;"
    @@where_delims_re = "GROUP BY|ORDER BY|LIMIT|PROCEDURE|INTO OUTFILE|INTO DUMPFILE|INTO|FOR UPDATE|LOCK IN SHARE MODE"
    @@operand_re = "([\"'`]?(\\w+)[\"'`]?(\\.[\"'`]?(\\w+)[\"'`]?)?)"
    @@binary_operators_re = "=|(>=)|(<>)|<|>|(<=)|(LIKE)|(IS)|(IS NOT)|(!=)|(IN)|(NOT IN)"
    @@expr_re = "((\\(|[ ])*(#{@@operand_re})[ ]*(#{@@binary_operators_re})[ ]*(#{@@operand_re})([ ]|\\))*?([ ]+((AND)|(OR))[ ]+)?)+"
        
    # Constructor
    def initialize(attributes = nil)
        @cur_db_attrs = {}
        rv = nil
        if attributes.nil?
            rv = super()
        else
            rv = super(attributes)
        end
        
        # use the built-in @attributes of ActiveRecord::Base as the version
        # of the hash that is updated when the user modifies the model.
        
        # the cur_db_attrs is the version of the hash that holds what is
        # currently in the database. It should only be updated during a
        # save operation or when a model is loaded from the db.
        @cur_db_attrs = attributes_from_column_definition()
        rv
    end

# public class methods
    def self.get_entity_field_name()
        efield_name = self.name.to_s().singularize().demodulize.underscore
        return efield_name + "_id"
    end
    
    # The get_sel_par_idxs() method obtains the indexes of the outer most
    # parenthesis pair that directly wraps a SELECT statement. If it finds
    # a SELECT statement wrapped in parenthesis then it returns a list
    # containing the index of the open paren and the close paren respectively
    # as the first and second item in the list. If it fails to find a SELECT
    # statement wrapped in parens then it returns nil.
    def self.get_sel_par_idxs(query)
        sel_idx = (/\([ ]*SELECT/i =~ query)
        if (sel_idx)
            par_count = 1
            cur_idx = sel_idx + 1
            last_par_idx = nil
            cur_par_idx = (/(\(|\))/ =~ query[cur_idx..-1])
            while (par_count > 0 && !cur_par_idx.nil?())
                if (query[(cur_idx + cur_par_idx)] == 40) # open paren
                    par_count += 1
                else
                    par_count -= 1
                end
                
                last_par_idx = cur_idx + cur_par_idx
                cur_idx = cur_idx + cur_par_idx + 1
                cur_par_idx = (/(\(|\))/i =~ query[cur_idx..-1])
            end
            ret_val = [sel_idx, last_par_idx]
            return ret_val
        else
            return nil
        end
    end
    
    def self.is_hist_table?(table_name)
      t_names = ["adm_locations", "adm_clients", "adm_hours", "adm_location_attachments", "adm_location_circuits", "adm_location_client_fields", "adm_location_ratings", "adm_location_visits", "ass_subcontracts", "ass_subcontract_details", "auth_permissions", "auth_roles", "auth_users", "bil_accounts", "con_assets", "con_components", "con_events", "hdw_inventories", "hdw_properties", "rdu_groups", "ass_campaigns"]
      return t_names.include?(table_name)
    end
    
    # Get a list of table names and aliases that are or reference tables that
    # would have the deleted fields.
    def self.get_table_names(query)
        t_names = ["adm_locations", "adm_clients", "adm_hours", "adm_location_attachments", "adm_location_circuits", "adm_location_client_fields", "adm_location_ratings", "adm_location_visits", "ass_subcontracts", "ass_subcontract_details", "auth_permissions", "auth_roles", "auth_users", "bil_accounts", "con_assets", "con_components", "con_events", "hdw_inventories", "hdw_properties", "rdu_groups", "ass_campaigns"]
        t_found = []      
        # find the FROM section and identify tables of interest and their
        # aliases if any and insert it or its alias into t_found
        if /FROM/i.match(query)
            # if table looking for exists
            t_names.each { |t|
                if /( |,|`)#{t}( |,|`)/.match(query)
                    # this table exists want to check if has alias
                    if /([ ]+|,|`)#{t}([ ]+|,|`)[ ]*(AS[ ]+)?(\w+)(,|[ ]*)/i.match(query)
                        cur_alias = $4
                        puts "cur_alias = \"#{cur_alias}\""
                        if !(/((INNER)|(CROSS)|(JOIN)|(STRAIGHT_JOIN)|(LEFT)|(RIGHT)|(ON)|(USING)|(NATURAL)|(OUTER)|(UNION)|(USE)|(IGNORE)|(FORCE)|(WHERE)|(GROUP)|(HAVING)|(ORDER)|(LIMIT)|(PROCEDURE)|(INTO)|(FOR)|(LIMIT))/.match(cur_alias))
                            puts "cur_alias DIDN'T match a keyword"
                            # if true we know has alias
                            # grab alias and put it in t_found
                            t_found.push(cur_alias)
                        else
                            puts "cur_alias DID match a keyword"
                            t_found.push(t) 
                        end
                    else
                        # it doesn't have an alias put the t in t_found
                        t_found.push(t)
                    end
                else
                   next
                end
            }
        end
        return t_found
    end
    
    # Take in the table references portion of a SQL query (the contents that
    # sit between the FROM and the WHERE or the FROM and the any other
    # keyword that follaws a WHERE if there is no WHERE) appropriately
    # inserts the (table_or_alias.deleted = 0 OR table_or_alias.deleted IS NULL)
    # and returns the resulting table references string.
    def self.ins_del_cons_in_table_refs(table_refs)
      int_table_refs = table_refs
      lt_del_const = nil
      cur_del_const = nil
      where_const = nil
      prev_join_type = nil
      cur_join_type = nil
      cur_where_table = nil
      there_were_joins = false
      dyn_const_list = []
      
      puts "ins_del_cons_in_table_refs: Starting table_refs: #{table_refs}"

      if !table_refs.nil?()
        # match the first left table as alias pair and add it to the dynamic
        # conditions building list.
        #if /^(([`"']?\w+['"`]?)(([ ]+AS[ ]+([`"']?\w+['"`]?))|([ ]+([`"']?\w+['"`]?)))??)([ ]+(#{@join_types_re}|#{@table_refs_delims_re}))?/i.match(int_table_refs) # matched the first left table as alais pair
        if /^([`"']?\w+[`"']?)((([ ]+(#{@@join_types_re}|#{@@table_refs_delims_re}))|[ ]*$)|([ ]+AS[ ]+([`"']?\w+[`"']?))|([ ]+([`"']?\w+[`"']?)))/i.match(int_table_refs)
          lt_name = $1
          lt_as_alias = $7
          lt_implicit_alias = $9
          puts "Matched: #{$&}"
          puts "Left Table Name: #{lt_name}"
          puts "Left Table As Alias: #{lt_as_alias}"
          puts "Left Table Implicit Alias: #{lt_implicit_alias}"
          

          cur_alias_or_name = nil
          if !lt_as_alias.nil?()
            cur_alias_or_name = lt_as_alias
          elsif !lt_implicit_alias.nil?()
            cur_alias_or_name = lt_implicit_alias
          else
            cur_alias_or_name = lt_name
          end
          
          if is_hist_table?(lt_name.gsub(/[`'"]/i, ""))
            lt_del_const = "(#{cur_alias_or_name}.deleted = 0 OR #{cur_alias_or_name}.deleted IS NULL)"
          else
            lt_del_const = nil
          end
        else # ERROR: This is a big problem (sign that the regex is missing a case)
          puts "\n\n\nSOME HUGE FREAKIN GUY (Failed to match first left table/table and alias)\n\n\n"
          return [nil, nil]
        end
        
        # At this point we should have cur_alias_or_name with the name of
        # the left most table in the query or its alias if it has one. If the
        # left most table happens to be a history table then set lt_del_const
        # to the propper constraint to strip out deleted.
        
        # match the nth join and right table as alaias pair and add it to the
        # dynamic conditions building list via a while loop.
        
        # FIXME: Add support to the regular expression to identify and match
        # either the ON or the USING keywords to pull out the conditions.
        # Because if we just do ON then it won't match any conditions even
        # though they exist and hence we won't have a necessary. It needs
        # to be parsed out and converted to an ON.        
        while /(#{@@join_types_re})[ ]+([`"']?\w+[`"']?)([ ]+ON[ ]+(#{@@expr_re}))?((([ ]+(#{@@join_types_re}|#{@@table_refs_delims_re}))|[ ]*$)|([ ]+AS[ ]+([`"']?\w+[`"']?)([ ]+ON[ ]+(#{@@expr_re}))?)|([ ]+([`"']?\w+[`"']?)([ ]+ON[ ]+(#{@@expr_re}))?))/i.match(int_table_refs)
          there_were_joins = true
          join_type = $1
          prev_join_type = cur_join_type
          cur_join_type = join_type
          rt_name = $2
          following_join_type = $35
          provided_join_expr = $4
          rt_as_alias = $37
          as_provided_join_expr = $39
          rt_implicit_alias = $68
          implicit_provided_join_expr = $70
          
          if !following_join_type.nil?()
            following_join_type = " #{following_join_type}"
          end
          
          if !rt_implicit_alias.nil?()
            provided_join_expr = implicit_provided_join_expr
            rt_orig_ref = "#{rt_name} #{rt_implicit_alias}"
          elsif !rt_as_alias.nil?()
            provided_join_expr = as_provided_join_expr
            rt_orig_ref = "#{rt_name} AS #{rt_as_alias}"
          else
            rt_orig_ref = rt_name
          end
          
          puts "Matched: #{$&}"
          puts "Join Type: #{join_type}"
          puts "Right Table Orig Ref: #{rt_orig_ref}"
          puts "Right Table Name: #{rt_name}"
          puts "Right Table As Alias: #{rt_as_alias}"
          puts "Right Table Implicit Alias: #{rt_implicit_alias}"
          puts "Provided Join Expr: #{provided_join_expr}"
          
          
          # Note: table_a, table_b is EQUIV to table_a INNER JOIN table_b without any constraints
          # Hence, I should be able to replace table_a, table_b with table_a INNER JOIN table_b ON constraints

          cur_alias_or_name = nil
          if !rt_as_alias.nil?()
            cur_alias_or_name = rt_as_alias
          elsif !rt_implicit_alias.nil?()
            cur_alias_or_name = rt_implicit_alias
          else
            cur_alias_or_name = rt_name
          end
          
          puts "Current Alias or Name: #{cur_alias_or_name}"
          
          cur_del_const = nil
          if is_hist_table?(rt_name.gsub(/[`'"]/i, ""))
            cur_del_const = "(#{cur_alias_or_name}.deleted = 0 OR #{cur_alias_or_name}.deleted IS NULL)"
          else
            cur_del_const = nil
          end
          
          puts "Cur Del Const After is_hist_table: #{cur_del_const}"
          
          if !lt_del_const.nil?() && !cur_del_const.nil?() # both not nil
            cur_del_const = "(#{lt_del_const} AND #{cur_del_const})"
            lt_del_const = nil
          elsif !lt_del_const.nil?() # cur_del_const is nil
            cur_del_const = "#{lt_del_const}"
          elsif !cur_del_const.nil?() # lt_del_const is nil
            cur_del_const = "#{cur_del_const}"
          else # both are nill
            cur_del_const = nil
          end

          puts "Cur Del Const After lt check: #{cur_del_const}"
          
          if !provided_join_expr.nil?() && !cur_del_const.nil?() # both not nil
            cur_del_const = "#{cur_del_const} AND (#{provided_join_expr})"
          elsif !provided_join_expr.nil?() # cur_del_const is nil
            cur_del_const = "#{provided_join_expr}"
          elsif !cur_del_const.nil?() # provided_join_expr is nil
            cur_del_const = "#{cur_del_const}"
          else # both are nil
            cur_del_const = nil
          end

          puts "Cur Del Const After provided expr: #{cur_del_const}"
          
          puts "Prev Join Type: '#{prev_join_type}'"
          puts "Cur Join Type: '#{cur_join_type}'"
          
          right_join_const_on = nil
          inner_join_const_on = nil
          if cur_join_type != prev_join_type
            if prev_join_type.nil?() && (cur_join_type == "INNER JOIN" || cur_join_type == ",")
              # don't need to handle anything special here for where
            elsif prev_join_type.nil?() && (cur_join_type == "LEFT JOIN" || cur_join_type == "LEFT OUTER JOIN")
              # want left table in where
              cur_where_table = lt_name
            elsif prev_join_type.nil?() && (cur_join_type == "RIGHT JOIN" || cur_join_type == "RIGHT OUTER JOIN")
              # want right table in where
              cur_where_table = rt_name
            elsif (prev_join_type == "INNER JOIN" || prev_join_type == ",") && (cur_join_type == "RIGHT JOIN" || cur_join_type == "RIGHT OUTER JOIN")
              # all INNER JOIN constraints go directly in their ON. Hence,
              # the constraints of the RIGHT JOIN just go in the WHERE clause.
              cur_where_table = rt_name
            elsif (prev_join_type == "INNER JOIN" || prev_join_type == ",") && (cur_join_type == "LEFT JOIN" || cur_join_type == "LEFT OUTER JOIN")
              # do nothing
            elsif (prev_join_type == "LEFT JOIN" || prev_join_type == "LEFT OUTER JOIN") && (cur_join_type == "RIGHT JOIN" || cur_join_type == "RIGHT OUTER JOIN")
              # prev left joins constraint table should be moved to the
              # current right joins ON statement
              right_join_const_on = cur_where_table
              cur_where_table = rt_name
            elsif (prev_join_type == "LEFT JOIN" || prev_join_type == "LEFT OUTER JOIN") && (cur_join_type == "INNER JOIN" || cur_join_type == ",")
              # prev left join constraint table should be moved ot the current
              # inner joins ON statement
              inner_join_const_on = cur_where_table
              cur_where_table = nil
            elsif (prev_join_type == "RIGHT JOIN" || prev_join_type == "RIGHT OUTER JOIN") && (cur_join_type == "LEFT JOIN" || cur_join_type == "LEFT OUTER JOIN")
              # prev right joins constraint table should STAY the constraint
              # table and should be included in the WHERE clause. Hence, do
              # nothing.
            elsif (prev_join_type == "RIGHT JOIN" || prev_join_type == "RIGHT OUTER JOIN") && (cur_join_type == "INNER JOIN" || cur_join_type == ",")
              # prev right joins constraint table should be moved to the
              # current inner joins ON statement
              inner_join_const_on = cur_where_table
              cur_where_table = nil
            end
          end
          
          puts "cur_where_table: '#{cur_where_table}'"
          puts "inner_join_const_on: '#{inner_join_const_on}'"
          puts "right_join_const_on: '#{right_join_const_on}'"
          
          if !inner_join_const_on.nil?() && !cur_del_const.nil?()
            # append const for inner join on statement
            if is_hist_table?(inner_join_const_on.gsub(/[`'"]/i, ""))
              cur_del_const = "(#{cur_del_const}) AND (#{inner_join_const_on}.deleted = 0 OR #{inner_join_const_on}.deleted IS NULL)"
            end
          elsif !inner_join_const_on.nil?() && cur_del_const.nil?()
            # create a new on statement
            if is_hist_table?(inner_join_const_on.gsub(/[`'"]/i, ""))
              cur_del_const = "(#{inner_join_const_on}.deleted = 0 OR #{inner_join_const_on}.deleted IS NULL)"
            end
          elsif !right_join_const_on.nil?() && !cur_del_const.nil?()
            # append the conts for the right join on statement
            if is_hist_table?(right_join_const_on.gsub(/[`'"]/i, ""))
              cur_del_const = "(#{cur_del_const}) AND (#{right_join_const_on}.deleted = 0 OR #{right_join_const_on}.deleted IS NULL)"
            end
          elsif !right_join_const_on.nil?() && cur_del_const.nil?()
            # create a new on statement
            if is_hist_table?(right_join_const_on.gsub(/[`'"]/i, ""))
              cur_del_const = "(#{right_join_const_on}.deleted = 0 OR #{right_join_const_on}.deleted IS NULL)"
            end
          end
          
          # Append inner or right join_const_on stuff to the current const
          # or create a new constraint if one doesn't exist
          # Basically need to rebuild the equiv of a modified table_refs
          # and then do a sub on int_query with the same regex used to
          # extract the table_refs and passing the new modified table_refs
          # as what to sub stitute it with. Note: In order for the while
          # loop method to work we need to substitute what was matched
          # in table_refs with a TOKEN KEYWORD (like HISTMODELCLAUSE) such
          # that the regex will then match the next element in the
          # table_refs.
          foo = nil
          if join_type == ","
            if !cur_del_const.nil?()
              foo = " INNER JOIN #{rt_orig_ref} ON #{cur_del_const}"
            else
              foo = " INNER JOIN #{rt_orig_ref}"
            end
          else
            if !cur_del_const.nil?()
              foo = " #{join_type} #{rt_orig_ref} ON #{cur_del_const}"
            else
              foo = " #{join_type} #{rt_orig_ref}"
            end
          end
          dyn_const_list.push(foo)
          
          int_table_refs = int_table_refs.sub(/(#{@@join_types_re})[ ]+([`"']?\w+[`"']?)([ ]+ON[ ]+(#{@@expr_re}))?((([ ]+(#{@@join_types_re}|#{@@table_refs_delims_re}))|[ ]*$)|([ ]+AS[ ]+([`"']?\w+[`"']?)([ ]+ON[ ]+(#{@@expr_re}))?)|([ ]+([`"']?\w+[`"']?)([ ]+ON[ ]+(#{@@expr_re}))?))/i, "HISTMODELCLAUSE#{following_join_type}")
        end # end of while loop
        
        if there_were_joins
          dyn_const_list.each { |constraint|
            int_table_refs = int_table_refs.sub(/HISTMODELCLAUSE/i, constraint)
          }
        else
          if !lt_del_const.nil?()
            cur_where_table = cur_alias_or_name
          end
        end
        
        puts "Int Table Refs: #{int_table_refs}"
        return [int_table_refs, cur_where_table]
      else # there are no table references
        puts "\n\n\nSOME HUGE FREAKIN GUY (Failed to find table references but found FROM)\n\n\n"
        return [nil, nil]
      end
    end
    
    def self.process_query(query)
      int_query = query
      from_list = []

      puts "\n\nStarting Query: #{query}"

      # Check if we have a FROM in the query. If we do we know that this should
      # be table references following it and potentially a bunch of other options
      # following the table references as seen by MySQL 5.1 SELECT syntax refence
      # (Ref1). If we can't match the FROM then we can assume that there are
      # no table rreferences and hence only prepended options and select
      # expressions.
      # Ref1: http://dev.mysql.com/doc/refman/5.1/en/select.html
      while /FROM/i.match(int_query) # Have a FROM should have table references following
        # Check if we have a FROM followed by table references followed by any
        # of the appendable options. If we do extract only the table references
        # as a regex group. If we DO NOT match any appendable options then we
        # assume that the all content after the FROM is table references.
        if /FROM[ ]+(.*?)[ ]+(#{@@table_refs_delims_re})/i.match(int_query) # Found table references followed by appendable options
          # have table references
          table_refs = $1
          table_refs_delims = $2
          puts "FOUND TABLE REF FOLLOWED BY OPTIONS"
          puts "Starting Table Refs: #{table_refs}"
          rs = ins_del_cons_in_table_refs(table_refs)
          new_table_refs = rs[0]
          where_table = rs[1]
          puts "Resulting Table Refs: #{new_table_refs}"
          from_list.push("FROM #{new_table_refs} #{table_refs_delims}")
          
          int_query = int_query.sub(/FROM[ ]+(.*?)[ ]+(#{@@table_refs_delims_re})/i, "HISTMODELSERIES")
        elsif /FROM[ ]+(.*)/i.match(int_query) # Could not find any appendable options, assuming all content after FROM is table referneces
          # have table references
          table_refs = $1
          puts "FOUND TABLE REF NOT FOLLOWED BY OPTIONS"
          puts "Starting Table Refs: #{table_refs}"
          rs = ins_del_cons_in_table_refs(table_refs)
          new_table_refs = rs[0]
          where_table = rs[1]
          puts "Resulting Table Refs: #{new_table_refs}"
          from_list.push("FROM #{new_table_refs}")
          
          int_query = int_query.sub(/FROM[ ]+(.*)/i, "HISTMODELSERIES")
        end
      end
      
      from_list.each { |from_entry|
        int_query = int_query.sub(/HISTMODELSERIES/i, from_entry)
      }
      
      # Match for the WHERE clause, should only be one, and pull out the
      # where clause content such that we can substitute it with a newly built
      # where clause.
      if !where_table.nil? # create and append one
        if /WHERE[ ]+(.*?)(([ ]+(#{@@where_delims_re}))|(;|$))/i.match(int_query)
          w_clause = $1
          w_delims = $3
          puts "Where Clause: '#{w_clause}'"
          puts "Where Delims: '#{w_delims}'"
          
          new_w_clause = "WHERE (#{w_clause}) AND (#{where_table}.deleted = 0 OR #{where_table}.deleted IS NULL)#{w_delims}"
          int_query = int_query.sub(/WHERE[ ]+(.*?)(([ ]+(#{@@where_delims_re}))|(;|$))/i, new_w_clause)
          
        elsif /(#{@@where_delims_re})/i.match(int_query) # create and append one
          w_delims = $1
          puts "Where Delims: '#{w_delims}'"
          new_w_clause = "WHERE (#{where_table}.deleted = 0 OR #{where_table}.deleted IS NULL)"
          int_query = int_query.sub(/(#{@@where_delims_re})/i, "#{new_w_clause} #{w_delims}")
        else # create and append one to the end
          new_w_clause = "WHERE (#{where_table}.deleted = 0 OR #{where_table}.deleted IS NULL)"
          int_query = "#{int_query} #{new_w_clause}"
        end
      end
      
      return int_query
    end
    
    def self.mod_or_create_on_clauses(query)
      puts "query = '#{query}'"
      join_tail_list = [] # list of join tails needed to proprely rebuild query
      int_query = query   # copy of the query for internal use and modification
      where_clause_tables = []
      
      
      
      # Match this first to get the first table and alias in the stuff between
      # the FROM and the potential WHERE.
      # ^(\w+)(([ ]+AS[ ]+(\w+))|([ ]+(\w+)))?([ ]+(JOIN|WHERE))?
      
      # Match this in a while loop to get the joins and the right tables and
      # aliases of each of those respective joins so that I can then build
      # the appropriate ON clauses.
      # (JOIN)[ ]+(\w+)(([ ]+AS[ ]+(\w+))|([ ]+(\w+)))?([ ]+ON[ ]+(expr))?

      if /FROM[ ]+(.*)/i.match(int_query)
        stuff_before_from = $`
        from_content = $1
        keywords_re = "(SELECT)|(FROM)|(INNER)|(CROSS)|(JOIN)|(STRAIGHT_JOIN)|(LEFT)|(RIGHT)|(ON)|(USING)|(NATURAL)|(OUTER)|(UNION)|(USE)|(IGNORE)|(FORCE)|(WHERE)|(GROUP)|(HAVING)|(ORDER)|(LIMIT)|(PROCEDURE)|(INTO)|(FOR)|(LIMIT)| |`|\"|'|,|\\.|="
        #operand_re = "('(.*?)')|(\\\"(.*?)\\\")|(`(.*?)`(\\.(`(.*?)`)?|(.*?))?)|((\\w|\\.)+)"
        operand_re = "([\"'`]?(\\w+)[\"'`]?(\\.[\"'`]?(\\w+)[\"'`]?)?)"
        join_types_re = "INNER JOIN|CROSS JOIN|STRAIGHT_JOIN|NATURAL LEFT OUTER JOIN|NATURAL RIGHT OUTER JOIN|NATURAL LEFT JOIN|NATURAL RIGHT JOIN|LEFT OUTER JOIN|RIGHT OUTER JOIN|LEFT JOIN|RIGHT JOIN|JOIN|,"
        #table_as_re = "[\"'`]?(\\w+)[\"'`]?([ ]+AS[ ]+[\"'`]?(\\w+)[\"'`]?)?"
        table_as_re = "[\"'`]?([^(#{keywords_re})]+)[\"'`]?([ ]+(AS[ ]+)?[\"'`]?([^(#{keywords_re})]+)[\"'`]?)?"
        binary_operators_re = "=|(>=)|(<>)|<|>|(<=)|(LIKE)|(IS)|(IS NOT)|(!=)|(IN)|(NOT IN)"
        expr_re = "((\\(|[ ])*(#{operand_re})[ ]*(#{binary_operators_re})[ ]*(#{operand_re})([ ]|\\))*?([ ]+((AND)|(OR))[ ]+)?)+"
        on_clause_re = "[ ]+ON[ ]+(#{expr_re})"
        while_re = "(#{table_as_re}[ ]*(#{join_types_re})[ ]*#{table_as_re}(#{on_clause_re})?)"
        str_rep_re = "((#{join_types_re})[ ]*#{table_as_re}(#{on_clause_re})?)"
        
        puts "operand_re = '#{operand_re}'"
        puts "expr_re = '#{expr_re}'"
        puts "table_as_re = '#{table_as_re}'"
        
        i = 0 # counter used solely to identify the first match
        while /#{while_re}/i.match(from_content)
          table_l_name = $2
          table_l_alias = $4
          join = $5
          table_r_name = $6
          table_r_alias = $8
          join_on = $9
          join_on_expr = $10
          has_hist_table = false

          puts "Iteration #{i} - matched: '#{$&}'"
          puts "Left Table Name = '#{table_l_name}'"
          puts "Left Table Alias = '#{table_l_alias}'"
          puts "Join = '#{join}'"
          puts "Right Table Name = '#{table_r_name}'"
          puts "Right Table Alias = '#{table_r_alias}'"
          puts "Join On = '#{join_on}'"
          puts "Join On Exp = '#{join_on_expr}'"
          puts "Group 1: #{$1}"
          puts "Group 3: #{$3}"
          puts "Group 7: #{$7}"

          new_stuff = "#{join} #{table_r_name}"
          if !table_r_alias.nil?
            new_stuff = new_stuff + " AS #{table_r_alias}"
          end

          if (i == 0)
            if is_hist_table?(table_l_name) && (join != ",")
              has_hist_table = true
              new_stuff = new_stuff + " ON ("
              if !table_l_alias.nil?
                new_stuff = new_stuff + "#{table_l_alias}.deleted = 0 OR #{table_l_alias}.deleted IS NULL"
              else
                new_stuff = new_stuff + "#{table_l_name}.deleted = 0 OR #{table_l_name}.deleted IS NULL"
              end
              new_stuff = new_stuff + ")"
            elsif is_hist_table?(table_l_name) && (join == ",")
              # should handle the deleted check in the where clause
              if !table_l_alias.nil?
                where_clause_tables.push(table_l_alias)
              else
                where_clause_tables.push(table_l_name)
              end
            end
          end

          if is_hist_table?(table_r_name) && (join != ",")
            if has_hist_table
              new_stuff = new_stuff + " AND"
            else
              new_stuff = new_stuff + " ON"
            end
            has_hist_table = true
            new_stuff = new_stuff + " ("
            if !table_r_alias.nil?
              new_stuff = new_stuff + "#{table_r_alias}.deleted = 0 OR #{table_r_alias}.deleted IS NULL"
            else
              new_stuff = new_stuff + "#{table_r_name}.deleted = 0 OR #{table_r_name}.deleted IS NULL"
            end
            new_stuff = new_stuff + ")"
          elsif is_hist_table?(table_r_name) && (join == ",")
            # should handle the deleted check in the where clause
            if !table_r_alias.nil?
              where_clause_tables.push(table_r_alias)
            else
              where_clause_tables.push(table_r_name)
            end
          end

          if !join_on.nil?
            if has_hist_table
              new_stuff = new_stuff + " AND"
            else
              new_stuff = new_stuff + " ON"
            end
            new_stuff = new_stuff + " (#{join_on_expr})"
          end
          puts "new_stuff = '#{new_stuff}'"
          join_tail_list.push(new_stuff)
          puts "before from_content = '#{from_content}'"
          from_content = from_content.sub(/#{str_rep_re}/i,"HISTCLAUSE ")
          puts "after from_content = '#{from_content}'"
          puts "\n\n"
          i = i + 1
        end

        puts "int_query = '#{int_query}'\n\n"
        puts "from_content = '#{from_content}'\n\n"
        puts "where_clause_tables = #{where_clause_tables.inspect()}\n\n"
        puts "join_tail_list = #{join_tail_list.inspect()}\n\n"

        i = 0
        join_tail_list.each { |s|
          puts "line #{i}: #{s}"
          from_content = from_content.sub(/HISTCLAUSE/, s)
          i = i + 1
        }

        int_query = stuff_before_from + "FROM " + from_content

        # do the WHERE processing

        if /WHERE[ ]+(#{expr_re})/i.match(int_query)
          puts "FOUND WHERE"
          where_exp = "#{$1}"
          new_where_exp = "WHERE"
          if (!where_clause_tables.empty?())
            i = 0
            where_clause_tables.each { |t|
              puts "HIT WHERE CLAUSE TABLE"
              if (i == 0)
                new_where_exp = new_where_exp + " (#{t}.deleted = 0 OR #{t}.deleted IS NULL)"
              else
                new_where_exp = new_where_exp + " AND (#{t}.deleted = 0 OR #{t}.deleted IS NULL)"
              end
              i = i + 1
            }
            if (i > 0)
              puts "ADDING ORIGINAL EXPRESSION"
              new_where_exp = new_where_exp + " AND (#{where_exp})"
            end
          else
            new_where_exp = new_where_exp + " #{where_exp}"
          end
        else
          if (!where_clause_tables.empty?())
            i = 0
            where_clause_tables.each { |t|
              if (i == 0)
                new_where_exp = new_where_exp + " (#{t}.deleted = 0 OR #{t}.deleted IS NULL)"
              else
                new_where_exp = new_where_exp + " AND (#{t}.deleted = 0 OR #{t}.deleted IS NULL)"
              end
              i = i + 1
            }
          end
        end

        puts "new_where_exp = '#{new_where_exp}'"
        # replace the WHERE clause with the new_where_exp
        int_query = int_query.sub(/WHERE[ ]+(#expr_re)/i, new_where_exp)

        puts "\n\n"
      end

      return int_query
    end


    def self.get_sub_query(query)
        # Note: need to add support for strings that contain new lines (multi-line mode)
        # Note: need to add support for strings that contain excess whitespace
        t_found = []
        lvl_sub_query_list = []
        
        top_lev_query = query
        
        # Matches all the pairs of the parens at this level and replaces them with (HISTMODELBASE)
        
        # Grab the paren indexes of the left most outer most sub SELECT statement at this level.
        cur_idxs = self.get_sel_par_idxs(top_lev_query)
        while !cur_idxs.nil?()
            # store the original sub query
            orig_sub_query = top_lev_query[cur_idxs[0]..cur_idxs[1]]
            
            lvl_sub_query_list.push(self.get_sub_query(orig_sub_query[1..-2]))
            # replace the sub SELECT with (HISTMODELBASE) in the top_level_query
            top_lev_query = top_lev_query[0..(cur_idxs[0] - 1)] + "(HISTMODELBASE)" + top_lev_query[(cur_idxs[1] + 1)..-1]
            # Grab the parens of the next left most sub SELECT
            cur_idxs = self.get_sel_par_idxs(top_lev_query)
        end
        
        # At this point I should have top_level_query with all of its sub
        # SELECT statements replaced with (HISTMODELBASE).
        
        # If we didn't match any of the HistModelBase tables (table which
        # would have deleted flag in them) then we replace the HISTMODELBASE
        # tag in the queries with the subquery that is produced by recursively
        # calling this function on the next level of subquery. Else, we
        # analyze the WHERE statement.
        t_found = self.get_table_names(top_lev_query)
        if t_found.empty?() # recursion end case
            lvl_sub_query_list.each { |q|
                top_lev_query = top_lev_query.sub(/HISTMODELBASE/, q)
            }
            return top_lev_query
        end
        
        # t_found has list of tables or aliases we should add delete = 0
        # constraints too at this point
        
        # find the WHERE and pull out the contents of the WHERE clause
        # so that we can wrap it in parens and prepend or append the
        # deleted = 0 condition appropriately and then put the query
        # back together
        
        # check if WHERE clause, if not one we need to figure out where
        # to put it. If one we do what the above comment says
        
        new_query = process_query(top_lev_query)
        # new_query = nil
        # if /WHERE[ ]+(((\(|[ ])*(('(.*?)')|("(.*?)")|(`(.*?)`(\.(`(.*?)`)?|(.*?))?)|((\w|\.)+))[ ]*(=|(>=)|(<>)|<|>|(<=)|(LIKE)|(IS)|(IS NOT)|(!=)|(IN)|(NOT IN))[ ]*(('(.*?)')|("(.*?)")|(\(.*?\))|(`(.*?)`(\.(`(.*?)`)?|(.*?)?))|((\w|\.)+))([ ]|\))*?([ ]+((AND)|(OR))[ ]+)?)+)/i.match(top_lev_query)
        #     # extract the contents of the WHERE clause NOT including the
        #     # /WHERE[ ]+/ part wrap it in parens and prepend or append
        #     # the deleted = 0 conditions appropriately, putting the query
        #     # back together
        #     #new_where = "WHERE " + t_found.join(".deleted = 0 AND ") + ".deleted = 0 AND (" + $1 + ")"
        #       new_where = "WHERE "
        #     t_found.each { |t|
        #         new_where += "(#{t}.deleted = 0 OR #{t}.deleted IS NULL) AND "
        #     }
        #     new_where += "(" + $1 +")"
        #     
        #     # know the stuff to the left of the match and the right
        #     # so use that to regenerate query
        #     new_query = $` + new_where + " " + $'
        #     
        #     puts "query = \"#{query}\""
        #     puts "new query = \"#{new_query}\""
        # else
        #     # we don't have a WHERE            
        #     #new_where = "WHERE " + t_found.join(".deleted = 0 AND ") + ".deleted = 0"
        #     new_where = "WHERE "
        #     first_iter = true
        #     t_found.each { |t|
        #         if (first_iter)
        #             new_where += "(#{t}.deleted = 0 OR #{t}.deleted IS NULL)"
        #             first_iter = false
        #         else
        #             new_where += " AND (#{t}.deleted = 0 OR #{t}.deleted IS NULL)"
        #         end
        #     }
        #     if /((GROUP BY)|(HAVING)|(ORDER BY)|(LIMIT)|(PROCEDURE)|(INTO)|(FOR)).*/i.match(top_lev_query)
        #         new_query = $` + new_where + " " + $&
        #     else
        #         new_query = top_lev_query + " " + new_where
        #     end
        # end
        lvl_sub_query_list.each { |q|
            new_query = new_query.sub(/HISTMODELBASE/, q)
        }
        return new_query
    end
    
    def self.ins_del_cond_in_sql(sql)
        if /deleted[ ]*(=|(>=)|<|>|(<=)|(IS)|(IS NOT)|(!=)|(<>)|(IN[ ]*\(.*?\))|(NOT IN[ ]*\(.*?\)))/i.match(sql)
            # you are in charge of your own destiny we just pass the query on bitches
            return sql
        else # the world is in our hands lets add our constraints
            return self.get_sub_query(sql)
        end
    end

    # Return the name of the name of the primary key of this table as it
    # would be referenced in other tables. Ex: If this was the AdmClient
    # table the return name would be adm_client_id as it is the field name
    # used in tables that reference this tables id.
    def self.get_table_id_name
        table_id_name = self.name.to_s().singularize().demodulize.underscore
        return table_id_name + "_id"
    end

    # Return the name of the change table that would be associated with this
    # model based on the models name.
    def self.get_change_table_name
        changetable_name = self.name.to_s().singularize().demodulize.underscore
        return changetable_name + "_changes"
    end

    # Return the user_id of the current session. This requires that the
    # associated application CPanel or AdSpot set the Thread local variable
    # as follows:
    #
    # Thread.current[:user_id] = session[:user_id]
    #
    # This is commonly done in the Merb::Controller via the before callback
    # as it happens at the beginning of each HTTP session before the
    # associated controler is called.
    def self.get_user_id
        uid = Thread.current[:user_id]
        if !uid.nil?()
            return uid
        else
            return 0
        end
    end
    
    # Retrun the sys_app_id of the application that the model is being used
    # in. This requires that the associated application CPanel or AdSpot set
    # the Thread local variable as follows:
    #
    # Thread.current[:sys_app_id] = RippleApp.sys_app_id
    #
    # This is commonly done in the Merb::Controller via the before callback
    # as it happens at the beginning of each HTTP session before the
    # associated controller is called.
    def self.get_sys_app_id
        sys_app_id = Thread.current[:sys_app_id]
        if !sys_app_id.nil?()
            return sys_app_id
        else
            return 0
        end
    end
    
    # Return a changeset hash based on the user_id and the current timestamp.
    # This is designed to be used by the client application in the cases
    # where you are going to be modifying multiple models but want them
    # included in the same changeset. You simply do this by calling this func
    # to get the changeset hash and then passing that changeset hash to the
    # save function calls of the models (including this one).
    def self.generate_changeset_hash
        sha1 = Digest::SHA1.hexdigest(get_user_id().to_s() + Time.new.to_s())
        return sha1
    end

    # Overloaded the create function so that can optional call it with a
    # changeset being passed to allow multiple model create/saves to happen
    # in the same changeset.
    def self.create(attributes = nil, changeset_hash = nil, &block)        
        if attributes.is_a?(Array)
            attributes.collect { |attr| create(attr, changeset_hash, &block) }
        else
            object = new(attributes)
            yield(object) if block_given?
            object.save(changeset_hash)
            object
        end
    end
    
    # Overloaded to set the cur_db_attrs after finding the data from the db.
    def self.find_by_sql(sql)
        # Note: All of the other find functions internally call this one so
        # the deleted = 0 conditions programatically get put in place for
        # those calls as well.        
        puts "FIND BY SQL SQL = '#{sql}'"
        sql = self.ins_del_cond_in_sql(self.sanitize_sql(sql))
        puts "RESULTING SQL = '#{sql}'"
        rv = super
        set_all_object_attrs(rv)
        rv
    end
    
    # Overloaded to set the cur_db_attrs after finding the data from the db.
    def self.reload(options = nil)
        rv = super
        set_all_object_attrs(rv)
        rv
    end

    # A class level utility function I wrote to iterate through a list of
    # objects and set that objects cur_db_attrs hash based on the values
    # in that objects attributes hash.
    def self.set_all_object_attrs(objs)
        if objs.is_a?(Array)
            objs.each { |obj|
                obj.set_db_attrs_from_attrs()
            }
        else
            if !objs.nil?
                objs.set_db_attrs_from_attrs()
            end
        end
    end
    
    def self.delete(id)
      r = find(id)
      if (r)
        r.destroy()
      end
    end

    def self.destroy(id)
      r = find(id)
      if (r)
        r.destroy()
      end
    end
    
    def self.destroy_all(conditions = nil)
      find(:all, :conditions => conditions).each { |object| object.destroy }
    end
    
    def self.delete_all(conditions = nil)
      find(:all, :conditions => conditions).each { |object| object.destroy }      
    end

# public instance methods

    # A instance level method that wraps the save functionality so that users
    # can save a model identifying that it is part of a changeset by passing
    # the changeset_hash or allow it to generate a changeset automatically.
    # Also this wrapper handles recording the historical changes for the
    # model.
    def save(changeset_hash = nil)
        rv = super() # call parents save function
        record_history(changeset_hash)
        rv
    end

    # A instance level method that wraps the save functionality so that users
    # can save a model identifying that it is part of a changeset by passing
    # the changeset_hash or allow it to generate a changeset automatically.
    # Also this wrapper handles recording the historical changes for the
    # model.
    def save!(changeset_hash = nil)
        rv = super() # call the parents save! function
        record_history(changeset_hash)
        rv
    end
    
    def destroy()
      # set the deleted flag field of the entity this object represents to
      # 1 so that it is seen as being deleted.
      self[:deleted] = 1
      # save the changes.
      save()
    end
    
    # An instance level utility function to set the cur_db_attrs hash based
    # on the instances attributes hash.
    def set_db_attrs_from_attrs()
        @cur_db_attrs = self.attributes()
    end
    
    # An instance level utility function to record the history of changes that
    # were made to the model. If a changeset_hash is provided as an argument
    # then that changeset_hash will be associated with these changes. If
    # no changeset_hash is passed it will automatically generate a new
    # changeset hash and associate the changes with that hash.
    def record_history(changeset_hash = nil)
        col_exclusions = ['created_on', 'updated_on']
        if changeset_hash.nil?
            sha1 = self.class.generate_changeset_hash()
        else
            sha1 = changeset_hash
        end
        
        # iterate through all the keys in the attribute hashes and compare
        # the values. If the values differ then I know that there is a
        # change and that change needs to be recorded in the appropriate
        # change table for this model.
        
        self.attributes().keys.each { |key|
            if !col_exclusions.include?(key)
                if self[key].to_s().strip() != @cur_db_attrs[key].to_s.strip()                
                    # generate the sql necessary to update the hestory for this entity
                    sql = "INSERT INTO #{self.class.get_change_table_name()} (#{self.class.get_entity_field_name()}, time, auth_user_id, field, old_value, new_value, changeset_hash, sys_application_type_id) VALUES (#{self.id}, NOW(), #{self.class.get_user_id}, '#{key.to_s()}', '#{connection.quote_string(@cur_db_attrs[key].to_s.strip())}', '#{connection.quote_string(@attributes[key].to_s().strip())}', '#{sha1}', #{self.class.get_sys_app_id});"
                
                    # run the generated sql to update the history
                    connection.execute(sql)
                end
            end
        }
        
        # Replace the old cur db values with the new ones.
        set_db_attrs_from_attrs()
    end
end