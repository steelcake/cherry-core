use proc_macro::TokenStream;
use quote::{quote, format_ident};
use syn::{parse_macro_input, DeriveInput, Data, Fields, Type};

#[proc_macro_derive(ToArrowSchema)]
pub fn derive_to_arrow_schema(input: TokenStream) -> TokenStream {
    // Parse the input tokens into a syntax tree
    let input = parse_macro_input!(input as DeriveInput);
    
    // Get the name of the struct
    let name = &input.ident;
    
    // Extract field information
    let fields = match &input.data {
        Data::Struct(data) => {
            match &data.fields {
                Fields::Named(fields) => {
                    fields.named.iter().map(|f| {
                        let field_name = f.ident.as_ref().unwrap().to_string();
                        let field_type = &f.ty;
                        
                        // Map Rust types to Arrow DataTypes
                        let arrow_type = match get_arrow_type(field_type) {
                            Some(arrow_type) => arrow_type,
                            None => quote! { DataType::Null },
                        };
                        
                        quote! {
                            Field::new(#field_name, #arrow_type, false)
                        }
                    }).collect::<Vec<_>>()
                },
                _ => panic!("ToArrowSchema only works on structs with named fields"),
            }
        },
        _ => panic!("ToArrowSchema only works on structs"),
    };
    
    // Generate the implementation
    let expanded = quote! {
        impl ToArrowSchema for #name {
            fn to_arrow_schema() -> Schema {
                let fields = vec![
                    #(#fields),*
                ];
                
                Schema::new(fields)
            }
        }
    };
    
    // Convert back to tokens
    TokenStream::from(expanded)
}

// Helper function to map Rust types to Arrow DataTypes
fn get_arrow_type(ty: &Type) -> Option<proc_macro2::TokenStream> {
    match ty {
        Type::Path(path) => {
            let last_segment = path.path.segments.last()?;
            let type_name = last_segment.ident.to_string();
            
            match type_name.as_str() {
                "u8" => Some(quote! { DataType::UInt8 }),
                "u16" => Some(quote! { DataType::UInt16 }),
                "u32" => Some(quote! { DataType::UInt32 }),
                "u64" => Some(quote! { DataType::UInt64 }),
                "i8" => Some(quote! { DataType::Int8 }),
                "i16" => Some(quote! { DataType::Int16 }),
                "i32" => Some(quote! { DataType::Int32 }),
                "i64" => Some(quote! { DataType::Int64 }),
                "f32" => Some(quote! { DataType::Float32 }),
                "f64" => Some(quote! { DataType::Float64 }),
                "bool" => Some(quote! { DataType::Boolean }),
                "String" => Some(quote! { DataType::Utf8 }),
                "Pubkey" => Some(quote! { DataType::Binary }),
                "Vec" => {
                    if last_segment.arguments.is_empty() {
                        return None;
                    }
                    Some(quote! { DataType::List(Box::new(Field::new("item", DataType::Binary, false))) })
                },
                _ => None,
            }
        },
        _ => None,
    }
}